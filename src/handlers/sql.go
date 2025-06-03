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
	"strings"
	"time"
)

type Item struct {
	src        string
	src_delete string
	dst        string
	aggr       []string
	period     uint64
	retention  uint64
}

func SqliteHandler(ich <-chan Datapoint) {

	ticker := time.NewTicker(3 * time.Minute)

	db := InitDB()
	if db != nil {
		CreateDispatchingTable(db)
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

func CreateDispatchingTable(db *sql.DB) bool {
	cmd := `
	CREATE TABLE IF NOT EXISTS dispatch (
		src_table TINYTEXT NOT NULL,
		src_delete TINYTEXT NOT NULL,
		dst_table TINYTEXT NOT NULL,
		aggr1 TINYTEXT NOT NULL,
		aggr2 TINYTEXT NOT NULL,
		aggr3 TINYTEXT NOT NULL,
		aggr4 TINYTEXT NOT NULL,
		period INT UNSIGNED NOT NULL,
		retention INT UNSIGNED NOT NULL
	);
	`
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create", "table", "dispatch", "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", "dispatch")
	return true
}

func CreateMeasurementTable(db *sql.DB, table string) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts DOUBLE NOT NULL,
		sensorid TINYTEXT NOT NULL,
		value DOUBLE NOT NULL,
		name TINYTEXT,
		place TINYTEXT
	);
	`
	cmd := fmt.Sprintf(cmdTemplate, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create", "table", table, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", table)
	return true
}

func CreateMeasurementIndex(db *sql.DB, table string) bool {
	cmdTemplate := `
	CREATE INDEX IF NOT EXISTS idx_%s_ts_sensorid_value
	ON %s (ts, sensorid);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create index", "table", table, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Index created", "table", table)
	return true
}

func mapNames(aggr []string, format string) (string, int) {
	cola := []string{}
	for _, f := range aggr {
		if f == "sum" || f == "min" || f == "max" || f == "avg" {
			c := fmt.Sprintf(format, f)
			cola = append(cola, c)
		}

	}
	cols := strings.Join(cola, ",")
	return cols, len(cols)
}

func CreateConsolidatedTable(db *sql.DB, item *Item) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts DOUBLE NOT NULL,
		sensorid TINYTEXT NOT NULL,
		%s,
		name TINYTEXT,
		place TINYTEXT
	);
	`
	cols, cnt := mapNames(item.aggr, "v%s DOUBLE NOT NULL")
	if cnt > 0 {
		cmd := fmt.Sprintf(cmdTemplate, item.dst, cols)
		_, err := db.Exec(cmd)
		if err != nil {
			slog.Error("Unable to create", "table", item.dst, "cmd", cmd, "err", err)
			return false
		}

		slog.Info("Table created", "table", item.dst)
	}
	return true
}

func CreateConsolidatedIndex(db *sql.DB, table string) bool {
	cmdTemplate := `
	CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_ts_sensorid
	ON %s (ts, sensorid);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, table)
	_, err := db.Exec(cmd)
	if err != nil {
		slog.Error("Unable to create index", "table", table, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Index created", "table", table)
	return true
}

func InsertMeasurement(db *sql.DB, dp *Datapoint) bool {
	cmdTemplate := `
	INSERT INTO %s (ts, sensorid, value, name, place) values(?, ?, ?, ?, ?);
	`
	table := fmt.Sprintf("measurements_%s", dp.Measurement)
	cmd := fmt.Sprintf(cmdTemplate, table)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Warn("Unable to prepare stmt", "table", table, "err", err1)
		if CreateMeasurementTable(db, table) && CreateMeasurementIndex(db, table) {
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err1)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	_, err2 := stmt.Exec(float64(dp.Timestamp)/1000.0, dp.Tags.ID, dp.Fields.Value, dp.Tags.Name, dp.Tags.Place)
	if err2 != nil {
		slog.Error("Insert error", "table", table, "data", dp, "err", err2)
		return false
	}

	slog.Debug("Inserted", "data", dp)
	return true
}

func InsertConsolidatedData(db *sql.DB, item *Item, ts uint64) bool {

	cmdTemplate := `
	REPLACE INTO %s (ts, sensorid, %s, name, place)
	SELECT FLOOR(ts/%d)*%d AS t, sensorid, %s, name, place
	FROM %s
	WHERE ts < %d
	GROUP BY t, sensorid, name, place;
	`
	cols, cnt := mapNames(item.aggr, "v%s")
	vals, _ := mapNames(item.aggr, "%s(value)")

	if cnt == 0 {
		return true
	}

	cmd := fmt.Sprintf(cmdTemplate, item.dst, cols, item.period, item.period, vals, item.src, ts)
	slog.Debug("Consolidation", "cmd", cmd)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Warn("Unable to prepare stmt", "table", item.dst, "err", err1)
		if CreateConsolidatedTable(db, item) && CreateConsolidatedIndex(db, item.dst) {
			stmt, err1 = db.Prepare(cmd)
			if err1 != nil {
				slog.Error("Unable to prepare stmt", "table", item.dst, "cmd", cmd, "err", err1)
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

	slog.Debug("Inserted", "table", item.dst, "ts", ts)
	return true
}

func DeleteData(db *sql.DB, table string, ts uint64) bool {
	cmdTemplate := `
	DELETE FROM %s WHERE ts < %d;
	`
	cmd := fmt.Sprintf(cmdTemplate, table, ts)
	stmt, err1 := db.Prepare(cmd)
	if err1 != nil {
		slog.Error("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err1)
		return false
	}
	defer stmt.Close()

	_, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Delete error", "table", table, "err", err2)
		return false
	}

	slog.Debug("Deleted", "table", table, "ts", ts)
	return true
}

func ConsolidateData(db *sql.DB) bool {
	cmd := `
	SELECT src_table, src_delete, dst_table, aggr1, aggr2, aggr3, aggr4, period, retention FROM dispatch;
	`

	rows, err1 := db.Query(cmd)
	if err1 != nil {
		slog.Error("Unable to query", "table", "dispatch", "err", err1)
		return false
	}

	var result []Item
	for rows.Next() {
		item := Item{}
		aggr := []string{"", "", "", ""}
		err2 := rows.Scan(&item.src,
			&item.src_delete,
			&item.dst,
			&aggr[0],
			&aggr[1],
			&aggr[2],
			&aggr[3],
			&item.period,
			&item.retention)
		item.aggr = aggr
		if err2 != nil {
			slog.Error("Unable to fetch", "table", "dispatch", "err", err1)
			return false
		}
		if item.period < 60 {
			item.period = 60
		}
		if item.retention > 0 && item.retention < 3600 {
			item.retention = 3600
		}
		slog.Info(
			"table dispatch",
			"src_table", item.src,
			"src_delete", item.src_delete,
			"dst_table", item.dst,
			"aggr1", item.aggr[0],
			"aggr2", item.aggr[1],
			"aggr3", item.aggr[2],
			"aggr4", item.aggr[3],
			"period", item.period,
			"retention", item.retention)
		result = append(result, item)
	}
	rows.Close()

	now := time.Now()
	for _, item := range result {
		if BeginTransaction(db) {
			ts := uint64(uint64((now.Unix()-20))/item.period) * item.period
			if !InsertConsolidatedData(db, &item, ts) {
				RollbackTransaction(db)
				continue
			}
			if item.src_delete == "yes" {
				if !DeleteData(db, item.src, ts) {
					RollbackTransaction(db)
					continue
				}
			}
			if item.retention > 0 {
				reten := ts - item.retention
				if !DeleteData(db, item.dst, reten) {
					RollbackTransaction(db)
					continue
				}
			}
			CommitTransaction(db)
		}
	}

	slog.Info("Consolidated", "now", now)
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
