/*
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  Copyright © 2025 Georges Ménie.
*/

package handlers

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log/slog"
	"strings"
	"time"
)

type Item struct {
	src        string
	src_delete string
	dst        string
	aggr       string
	period     float64
}

func SqliteHandler(ich <-chan Datapoint) {

	ticker := time.NewTicker(3 * time.Minute)

	db := InitDB("/data/measurements.db")
	if db != nil {
		CreateManagementTable(db)
		for {
			select {
			case dp := <-ich:
				slog.Debug(
					"json parsed",
					"measurement", dp.Measurement,
					"ts", dp.Timestamp,
					"id", dp.Tags.ID,
					"name", dp.Tags.Name,
					"place", dp.Tags.Place,
					"value", dp.Fields.Value)
				InsertMeasurement(db, &dp)
			case t := <-ticker.C:
				slog.Debug("Tick", "at", t)
				ConsolidateData(db)
			}
		}
		db.Close()
	}

	ticker.Stop()
}

func InitDB(filepath string) *sql.DB {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		slog.Error("Unable to open database", "filepath", filepath, "err", err)
	} else {
		slog.Info("Database opened", "filepath", filepath)
	}
	return db
}

// delete from management;
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_energy_watthourdiff', 'yes', 'energy_watthourdiff_5m', 'sum', 300);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_energy_watthour', 'yes', 'energy_watthour_5m', 'min', 300);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_tension_volt', 'yes', 'tension_volt_20m', 'avg', 1200);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_temperature_celsius', 'no', 'temperature_celsius_min_1h', 'min', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_temperature_celsius', 'no', 'temperature_celsius_max_1h', 'max', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_temperature_celsius', 'yes', 'temperature_celsius_avg_1h', 'avg', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_humidity_pct', 'no', 'humidity_pct_min_1h', 'min', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_humidity_pct', 'no', 'humidity_pct_max_1h', 'max', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_humidity_pct', 'yes', 'humidity_pct_avg_1h', 'avg', 3600);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_power_va', 'yes', 'power_va_1m', 'avg', 60);
// insert into management (src_table, src_delete, dst_table, aggregate, period) values('measurements_power_watt', 'yes', 'power_watt_1m', 'avg', 60);
func CreateManagementTable(db *sql.DB) {
	cmd := `
	CREATE TABLE IF NOT EXISTS management (
		src_table TEXT,
		src_delete TEXT,
		dst_table TEXT,
		aggregate TEXT,
		period REAL
	);
	`
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create", "table", "management", "err", err)
	} else {
		slog.Info("Table created", "table", "management")
	}
}

func CreateMeasurementsTable(db *sql.DB, table string) {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		sensorid TEXT,
		name TEXT,
		place TEXT,
		ts REAL,
		value REAL
	);
	`
	cmd := fmt.Sprintf(cmdTemplate, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create", "table", table, "err", err)
	} else {
		slog.Info("Table created", "table", table)
	}
}

func CreateMeasurementsIndex(db *sql.DB, table string) {
	cmdTemplate := `
	CREATE UNIQUE INDEX IF NOT EXISTS idx_%s
	ON %s (sensorid, name, place, ts);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create index", "table", table, "err", err)
	} else {
		slog.Info("Index created", "table", table)
	}
}

// lire avec
// select datetime(ts,'unixepoch','localtime'),sensorid,name,value from measurements_energy_watthourdiff order by name, ts;
func InsertMeasurement(db *sql.DB, dp *Datapoint) {
	cmdTemplate := `
	INSERT INTO %s (sensorid, name, place, ts, value) values(?, ?, ?, ?, ?);
	`
	table := fmt.Sprintf("measurements_%s", dp.Measurement)
	cmd := fmt.Sprintf(cmdTemplate, table)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		if strings.Contains(err1.Error(), "no such table") {
			CreateMeasurementsTable(db, table)
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", table, "err", err1)
				return
			}
		} else {
			slog.Error("Unable to prepare stmt", "table", table, "err", err1)
			return
		}
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(dp.Tags.ID, dp.Tags.Name, dp.Tags.Place, float64(dp.Timestamp)/1000.0, dp.Fields.Value)
	if err2 != nil {
		slog.Error("Insert error", "table", table, "err", err2)
	} else {
		slog.Debug("Inserted", "datapoint", dp)
	}
}

func DeleteMeasurement(db *sql.DB, item *Item, ts int64) {
	cmdTemplate := `
	DELETE FROM %s WHERE ts < CAST(%d/%.1f AS INTEGER)*%.1f;
	`
	cmd := fmt.Sprintf(cmdTemplate, item.src, ts, item.period, item.period)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Error("Unable to prepare stmt", "table", item.src, "err", err1)
		return
	}
	defer stmt.Close()

	_, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Delete error", "table", item.src, "err", err2)
	} else {
		slog.Debug("Deleted", "table", item.src, "ts", ts)
	}
}

func InsertConsolidatedData(db *sql.DB, item *Item, ts int64) {

	cmdTemplate := `
	INSERT OR REPLACE INTO %s (sensorid, name, place, ts, value)
	SELECT sensorid, name, place, CAST(ts/%.1f AS INTEGER)*%.1f as ts, %s(value)
	FROM %s
	WHERE ts < CAST(%d/%.1f AS INTEGER)*%.1f
	GROUP BY sensorid, name, place, CAST(ts/%.1f AS INTEGRER);
	`
	cmd := fmt.Sprintf(cmdTemplate, item.dst, item.period, item.period, item.aggr, item.src, ts, item.period, item.period, item.period)
	slog.Debug("Consolidation", "cmd", cmd)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		if strings.Contains(err1.Error(), "no such table") {
			CreateMeasurementsTable(db, item.dst)
			CreateMeasurementsIndex(db, item.dst)
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", item.dst, "err", err1)
				return
			}
		} else {
			slog.Error("Unable to prepare stmt", "table", item.dst, "err", err1)
			return
		}
	}
	defer stmt.Close()

	_, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Insert error", "table", item.dst, "err", err2)
	} else {
		slog.Debug("Inserted", "table", item.dst)
	}
}

func ConsolidateData(db *sql.DB) {
	cmd := `
	SELECT src_table, src_delete, dst_table, aggregate, period FROM management;
	`

	rows, err1 := db.Query(cmd)
	if err1 != nil {
		slog.Error("Unable to query", "table", "management", "err", err1)
		return
	}

	var result []Item
	for rows.Next() {
		item := Item{}
		err2 := rows.Scan(&item.src, &item.src_delete, &item.dst, &item.aggr, &item.period)
		if err2 != nil {
			slog.Error("Unable to fetch", "table", "management", "err", err1)
			return
		}
		slog.Info(
			"table management",
			"src_table", item.src,
			"src_delete", item.src_delete,
			"dst_table", item.dst,
			"aggregate", item.aggr,
			"period", item.period)
		result = append(result, item)
	}
	rows.Close()

	now := time.Now()
	for _, item := range result {
		InsertConsolidatedData(db, &item, now.Unix())
		if item.src_delete == "yes" {
			DeleteMeasurement(db, &item, now.Unix())
		}
	}
	slog.Info("Consolidated", "now", now, "ts", now.Unix())
}
