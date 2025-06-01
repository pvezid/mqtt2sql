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
	_ "github.com/go-sql-driver/mysql"
	"log/slog"
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

	db := InitDB()
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

func InitDB() *sql.DB {
	db, err := sql.Open("mysql", "ustd:m55PC2Qh@tcp(mariadb:3306)/mqtt2sql")
	if err != nil {
		slog.Error("Unable to open database", "err", err)
		return nil
	}

	slog.Info("Database opened")
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
func CreateManagementTable(db *sql.DB) bool {
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
		return false
	}

	slog.Info("Table created", "table", "management")
	return true
}

func CreateMeasurementsTable(db *sql.DB, table string) bool {
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
		return false
	}

	slog.Info("Table created", "table", table)
	return true
}

func CreateMeasurementsIndex(db *sql.DB, table string) bool {
	cmdTemplate := `
	CREATE INDEX IF NOT EXISTS idx_%s_ts
	ON %s (ts);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create index", "table", table, "err", err)
		return false
	}

	slog.Info("Index created", "table", table)
	return true
}

// lire avec
// select datetime(ts,'unixepoch','localtime'),sensorid,name,value from measurements_energy_watthourdiff order by name, ts;
func InsertMeasurement(db *sql.DB, dp *Datapoint) bool {
	cmdTemplate := `
	INSERT INTO %s (sensorid, name, place, ts, value) values(?, ?, ?, ?, ?);
	`
	table := fmt.Sprintf("measurements_%s", dp.Measurement)
	cmd := fmt.Sprintf(cmdTemplate, table)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Warn("Unable to prepare stmt", "table", table, "err", err1)
		if CreateMeasurementsTable(db, table) {
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", table, "err", err1)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(dp.Tags.ID, dp.Tags.Name, dp.Tags.Place, float64(dp.Timestamp)/1000.0, dp.Fields.Value)
	if err2 != nil {
		slog.Error("Insert error", "table", table, "err", err2)
		return false
	}

	slog.Debug("Inserted", "datapoint", dp)
	return true
}

func DeleteMeasurement(db *sql.DB, item *Item, ts int64) bool {
	cmdTemplate := `
	DELETE FROM %s WHERE ts < CAST(%d/%.0f AS UNSIGNED)*%.0f;
	`
	cmd := fmt.Sprintf(cmdTemplate, item.src, ts, item.period, item.period)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Error("Unable to prepare stmt", "table", item.src, "err", err1)
		return false
	}
	defer stmt.Close()

	_, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Delete error", "table", item.src, "err", err2)
		return false
	}

	slog.Debug("Deleted", "table", item.src, "ts", ts)
	return true
}

func InsertConsolidatedData(db *sql.DB, item *Item, ts int64) bool {

	cmdTemplate := `
	INSERT INTO %s (sensorid, name, place, ts, value)
	SELECT sensorid, name, place, CAST(ts/%.0f AS UNSIGNED)*%.0f as ts, %s(value)
	FROM %s
	WHERE ts < CAST(%d/%.0f AS UNSIGNED)*%.0f
	GROUP BY sensorid, name, place, CAST(ts/%.0f AS UNSIGNED);
	`
	cmd := fmt.Sprintf(cmdTemplate, item.dst, item.period, item.period, item.aggr, item.src, ts, item.period, item.period, item.period)
	slog.Debug("Consolidation", "cmd", cmd)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Warn("Unable to prepare stmt", "table", item.dst, "err", err1)
		if CreateMeasurementsTable(db, item.dst) && CreateMeasurementsIndex(db, item.dst) {
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", item.dst, "err", err1)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	_, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Insert error", "table", item.dst, "err", err2)
		return false
	}

	slog.Debug("Inserted", "table", item.dst)
	return true
}

func ConsolidateData(db *sql.DB) bool {
	cmd := `
	SELECT src_table, src_delete, dst_table, aggregate, period FROM management;
	`

	rows, err1 := db.Query(cmd)
	if err1 != nil {
		slog.Error("Unable to query", "table", "management", "err", err1)
		return false
	}

	var result []Item
	for rows.Next() {
		item := Item{}
		err2 := rows.Scan(&item.src, &item.src_delete, &item.dst, &item.aggr, &item.period)
		if err2 != nil {
			slog.Error("Unable to fetch", "table", "management", "err", err1)
			return false
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
	ts := now.Unix()
	for _, item := range result {
		if BeginTransaction(db) {
			if InsertConsolidatedData(db, &item, ts) {
				if item.src_delete == "yes" {
					if DeleteMeasurement(db, &item, ts) {
						CommitTransaction(db)
					} else {
						RollbackTransaction(db)
					}
				} else {
					CommitTransaction(db)
				}
			} else {
				RollbackTransaction(db)
			}
		}
	}

	slog.Info("Consolidated", "now", now, "ts", ts)
	return true
}

func BeginTransaction(db *sql.DB) bool {
	_, err := db.Exec("START TRANSACTION")
	if err != nil {
		slog.Error("Unable to start a transaction", "err", err)
		return false
	}
	return true
}

func CommitTransaction(db *sql.DB) bool {
	_, err := db.Exec("COMMIT")
	if err != nil {
		slog.Error("Unable to commit a transaction", "err", err)
		return false
	}
	return true
}

func RollbackTransaction(db *sql.DB) bool {
	_, err := db.Exec("ROLLBACK")
	if err != nil {
		slog.Error("Unable to rollback a transaction", "err", err)
		return false
	}
	return true
}
