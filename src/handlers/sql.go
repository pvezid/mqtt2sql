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
	"slices"
	"strings"
	"time"
)

const connString = "ustd:m55PC2Qh@tcp(mariadb:3306)/mqtt2sql"
const dispatchTable = "dispatch"
const DefaultCol = "value"
const MargeTemps = 40

type Item struct {
	src        string
	src_delete string
	dst        string
	aggr       []string
	alist      string
	clist      string
	dlist      string
	period     uint64
	retention  uint64
}

var lastBrowsed map[string]uint64

type Index struct {
	nom   string
	attr  string
	table string
	cols  string
}

func SqliteHandler(ich <-chan Datapoint) {

	ticker := time.NewTicker(3 * time.Minute)
	lastBrowsed = make(map[string]uint64)

	db := InitDB()
	if db != nil {
		defer db.Close()
		// results not used, this is to create the table as early as possible
		ReadOrCreateDispatchingTable(db)
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
				if items, ok := ReadOrCreateDispatchingTable(db); ok {
					for _, item := range items {
						if maxts, ok := ReadMaxTimestamp(db, item.dst); ok {
							lastBrowsed[item.dst] = uint64((uint64(maxts)/item.period)+1) * item.period
						} else {
							lastBrowsed[item.dst] = 0
						}
					}
					ConsolidateData(db, items)
				}
			}
		}
	}

	ticker.Stop()
}

func InitDB() *sql.DB {
	if db, err := sql.Open("mysql", connString); err != nil {
		slog.Error("Unable to open database", "err", err)
		return nil
	} else {
		slog.Info("Database opened")
		return db
	}
}

func ReadOrCreateDispatchingTable(db *sql.DB) ([]Item, bool) {
	items, err := ReadDispatchingTable(db)
	if err != nil {
		slog.Warn("Unable to query", "table", dispatchTable, "err", err)
		if CreateDispatchingTable(db) && CreateDispatchingIndex(db) {
			if items, err = ReadDispatchingTable(db); err != nil {
				slog.Error("Unable to query", "table", dispatchTable, "err", err)
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return items, true
}

func CreateDispatchingTable(db *sql.DB) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		rank INT UNSIGNED NOT NULL,
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
	cmd := fmt.Sprintf(cmdTemplate, dispatchTable)
	if _, err := db.Exec(cmd); err != nil {
		slog.Error("Unable to create", "table", dispatchTable, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", dispatchTable)
	return true
}

func CreateDispatchingIndex(db *sql.DB) bool {
	indexes := []Index{
		Index{"idxdisp_rank", "UNIQUE", dispatchTable, "rank"},
		Index{"idxdisp_dst_table", "UNIQUE", dispatchTable, "dst_table"},
	}
	return CreateIndexes(db, indexes)
}

func ConsolidateData(db *sql.DB, items []Item) bool {
	now := time.Now()
	for _, item := range items {
		item.clist = mapNames(item.aggr, []string{}, "v%s")
		item.dlist = mapNames(item.aggr, []string{}, "v%s DOUBLE NOT NULL")
		idx := slices.IndexFunc(items, func(i Item) bool { return i.dst == item.src })
		if idx >= 0 {
			item.alist = mapNames(item.aggr, items[idx].aggr, "%%s(v%s)")
		} else {
			item.alist = mapNames(item.aggr, []string{DefaultCol, DefaultCol, DefaultCol, DefaultCol}, "%%s(%s)")
		}
		t2 := uint64(uint64((now.Unix()-MargeTemps))/item.period) * item.period
		t1 := lastBrowsed[item.dst]
		slog.Debug(
			"dispatching",
			"src_table", item.src,
			"src_delete", item.src_delete,
			"dst_table", item.dst,
			"aggr", strings.Join(item.aggr, ", "),
			"alist", item.alist,
			"clist", item.clist,
			"dlist", item.dlist,
			"period", item.period,
			"retention", item.retention,
			"last_ts", t1,
			"ts", t2)
		if len(item.alist) == 0 || t2 <= t1 {
			continue
		}
		slog.Info(
			"dispatching",
			"src_table", item.src,
			"src_delete", item.src_delete,
			"dst_table", item.dst,
			"alist", item.alist,
			"period", item.period,
			"retention", item.retention,
			"last_ts", t1,
			"ts", t2)
		if BeginTransaction(db) {
			if !InsertConsolidatedData(db, item, t1, t2) {
				RollbackTransaction(db)
				continue
			}
			if item.src_delete == "yes" {
				if !DeleteData(db, item.src, t1, t2) {
					RollbackTransaction(db)
					continue
				}
			}
			if item.retention > 0 && t2 > item.retention {
				if !DeleteData(db, item.dst, 0, t2-item.retention) {
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

func CreateMeasurementTable(db *sql.DB, table string) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts DOUBLE NOT NULL,
		sensorid TINYTEXT NOT NULL,
		%s DOUBLE NOT NULL,
		name TINYTEXT,
		place TINYTEXT
	);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, DefaultCol)
	if _, err := db.Exec(cmd); err != nil {
		slog.Error("Unable to create", "table", table, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", table)
	return true
}

func CreateMeasurementIndex(db *sql.DB, table string) bool {
	indexes := []Index{
		Index{"idxmeas_ts_sensorid", "", table, "ts, sensorid"},
	}
	return CreateIndexes(db, indexes)
}

func CreateConsolidatedTable(db *sql.DB, item Item) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts DOUBLE NOT NULL,
		sensorid TINYTEXT NOT NULL,
		%s,
		name TINYTEXT,
		place TINYTEXT
	);
	`
	cmd := fmt.Sprintf(cmdTemplate, item.dst, item.dlist)
	if _, err := db.Exec(cmd); err != nil {
		slog.Error("Unable to create", "table", item.dst, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", item.dst)
	return true
}

func CreateConsolidatedIndex(db *sql.DB, table string) bool {
	indexes := []Index{
		Index{"idxcons_ts_sensorid_name_place", "UNIQUE", table, "ts, sensorid, name, place"},
	}
	return CreateIndexes(db, indexes)
}

func ReadDispatchingTable(db *sql.DB) ([]Item, error) {
	cmdTemplate := `
	SELECT src_table, src_delete, dst_table, aggr1, aggr2, aggr3, aggr4, period, retention FROM %s ORDER BY rank;
	`
	cmd := fmt.Sprintf(cmdTemplate, dispatchTable)
	rows, err := db.Query(cmd)
	if err != nil {
		return nil, err
	}

	var result []Item
	for rows.Next() {
		item := Item{}
		aggr := []string{"", "", "", ""}
		err := rows.Scan(&item.src,
			&item.src_delete,
			&item.dst,
			&aggr[0],
			&aggr[1],
			&aggr[2],
			&aggr[3],
			&item.period,
			&item.retention)
		item.aggr = aggr
		if err != nil {
			slog.Error("Unable to fetch", "table", dispatchTable, "err", err)
			continue
		}
		if item.period > 0 {
			item.period *= 60
			item.retention *= 60
			result = append(result, item)
		}
	}
	rows.Close()

	return result, nil
}

func ReadMaxTimestamp(db *sql.DB, table string) (float64, bool) {
	cmdTemplate := `
	SELECT max(ts) FROM %s;
	`
	cmd := fmt.Sprintf(cmdTemplate, table)
	rows, err := db.Query(cmd)
	if err != nil {
		return 0, false
	}

	var maxts sql.NullFloat64
	for rows.Next() {
		err := rows.Scan(&maxts)
		if err != nil {
			slog.Error("Unable to fetch", "table", table, "err", err)
			return 0, false
		}
		break
	}
	rows.Close()

	if maxts.Valid {
		return maxts.Float64, true
	} else {
		return 0, false
	}
}

func InsertMeasurement(db *sql.DB, dp *Datapoint) bool {
	cmdTemplate := `
	INSERT INTO %s (ts, sensorid, %s, name, place) values (?, ?, ?, ?, ?);
	`
	table := fmt.Sprintf("measurements_%s", dp.Measurement)
	cmd := fmt.Sprintf(cmdTemplate, table, DefaultCol)
	stmt, err := db.Prepare(cmd)
	if err != nil {
		slog.Warn("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err)
		if CreateMeasurementTable(db, table) && CreateMeasurementIndex(db, table) {
			if stmt, err = db.Prepare(cmd); err != nil {
				slog.Error("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	if _, err := stmt.Exec(float64(dp.Timestamp)/1000.0, dp.Tags.ID, dp.Fields.Value, dp.Tags.Name, dp.Tags.Place); err != nil {
		slog.Error("Insert error", "table", table, "data", dp, "err", err)
		return false
	}

	slog.Debug("Inserted", "data", dp)
	return true
}

func InsertConsolidatedData(db *sql.DB, item Item, t1 uint64, t2 uint64) bool {

	cmdTemplate := `
	INSERT INTO %s (ts, sensorid, %s, name, place)
	SELECT FLOOR(ts/%d)*%d AS t, sensorid, %s, name, place
	FROM %s
	WHERE ts >= %d AND ts < %d
	GROUP BY t, sensorid, name, place;
	`

	cmd := fmt.Sprintf(cmdTemplate, item.dst, item.clist, item.period, item.period, item.alist, item.src, t1, t2)
	slog.Debug("Consolidation", "cmd", cmd)
	stmt, err := db.Prepare(cmd)
	if err != nil {
		slog.Warn("Unable to prepare stmt", "table", item.dst, "cmd", cmd, "err", err)
		if CreateConsolidatedTable(db, item) && CreateConsolidatedIndex(db, item.dst) {
			if stmt, err = db.Prepare(cmd); err != nil {
				slog.Error("Unable to prepare stmt", "table", item.dst, "cmd", cmd, "err", err)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	if _, err2 := stmt.Exec(); err2 != nil {
		slog.Error("Insert error", "table", item.dst, "err", err2)
		return false
	}

	slog.Debug("Inserted", "table", item.dst, "t1", t1, "t2", t2)
	return true
}

func DeleteData(db *sql.DB, table string, t1 uint64, t2 uint64) bool {
	cmdTemplate := `
	DELETE FROM %s WHERE ts >= %d AND ts < %d;
	`
	cmd := fmt.Sprintf(cmdTemplate, table, t1, t2)
	stmt, err := db.Prepare(cmd)
	if err != nil {
		slog.Error("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err)
		return false
	}
	defer stmt.Close()

	if _, err := stmt.Exec(); err != nil {
		slog.Error("Delete error", "table", table, "err", err)
		return false
	}

	slog.Debug("Deleted", "table", table, "t1", t1, "t2", t2)
	return true
}

func BeginTransaction(db *sql.DB) bool {
	if _, err := db.Exec("START TRANSACTION"); err != nil {
		slog.Error("Unable to start a transaction", "err", err)
		return false
	}
	return true
}

func CommitTransaction(db *sql.DB) bool {
	if _, err := db.Exec("COMMIT"); err != nil {
		slog.Error("Unable to commit a transaction", "err", err)
		return false
	}
	return true
}

func RollbackTransaction(db *sql.DB) bool {
	if _, err := db.Exec("ROLLBACK"); err != nil {
		slog.Error("Unable to rollback a transaction", "err", err)
		return false
	}
	return true
}

func CreateIndexes(db *sql.DB, indexes []Index) bool {
	ret := true
	cmdTemplate := "CREATE %s INDEX IF NOT EXISTS %s ON %s (%s)"

	for _, idx := range indexes {
		cmd := fmt.Sprintf(cmdTemplate, idx.attr, idx.nom, idx.table, idx.cols)
		if _, err := db.Exec(cmd); err != nil {
			slog.Error("Unable to create index", "cmd", cmd, "err", err)
			ret = false
		} else {
			slog.Info("Index created", "table", idx.table, "index", idx.nom)
		}
	}

	return ret
}

func mapNames(aggr []string, col []string, format string) string {
	cola := []string{}
	for i, f := range aggr {
		if f == "sum" || f == "min" || f == "max" || f == "avg" {
			var subformat string
			if len(col) > 0 {
				subformat = fmt.Sprintf(format, col[i])
			} else {
				subformat = format
			}
			c := fmt.Sprintf(subformat, f)
			cola = append(cola, c)
		}

	}
	cols := strings.Join(cola, ", ")
	return cols
}
