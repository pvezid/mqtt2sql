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

type Item struct {
	src        string
	src_delete string
	dst        string
	aggr       []string
	alist      string
	clist      string
	dlist      string
	period     int64
	retention  int64
}

type Index struct {
	nom   string
	attr  string
	table string
	cols  string
}

type DB struct {
	*sql.DB
}

const (
	connString      = "ustd:m55PC2Qh@tcp(mariadb:3306)/mqtt2sql"
	dispatchTable   = "dispatch"
	defaultCol      = "value"
	margeTemps      = 40
	measurementTmpl = "measurements_%s"
)

var (
	lastBrowsed  map[string]int64
	measReceived map[string]int64
)

func SqlBatchHandler(ich <-chan Datapoint) {
	cmdTemplate := "INSERT INTO %s (ts, sensorid, %s, name, place) values (%.3f, '%s', %v, '%s', '%s'); -- %v"

	for dp := range ich {
		table := fmt.Sprintf(measurementTmpl, dp.Measurement)
		cmd := fmt.Sprintf(cmdTemplate, table, defaultCol, float64(dp.Timestamp)/1000.0, dp.Tags.ID, dp.Fields.Value, dp.Tags.Name, dp.Tags.Place, time.UnixMilli(dp.Timestamp))
		fmt.Println(cmd)
	}
}

func SqlHandler(ich <-chan Datapoint) {

	ticker := time.NewTicker(3 * time.Minute)
	lastBrowsed = make(map[string]int64)
	measReceived = make(map[string]int64)

	db := newDB()
	if db != nil {
		defer db.Close()
		// results not used, this is to create the table as early as possible
		db.ReadOrCreateDispatchingTable()
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
				db.InsertMeasurement(&dp)
			case t := <-ticker.C:
				slog.Debug("Tick", "at", t)
				if items, ok := db.ReadOrCreateDispatchingTable(); ok {
					db.ConsolidateData(items)
				}
			}
		}
	}

	ticker.Stop()
}

func newDB() *DB {
	if db, err := sql.Open("mysql", connString); err != nil {
		slog.Error("Unable to open database", "err", err)
		return nil
	} else {
		slog.Info("Database opened")
		return &DB{db}
	}
}

func (db *DB) ReadOrCreateDispatchingTable() ([]Item, bool) {
	items, err := db.ReadDispatchingTable()
	if err != nil {
		slog.Warn("Unable to query", "table", dispatchTable, "err", err)
		if db.CreateDispatchingTable() && db.CreateDispatchingIndex() {
			if items, err = db.ReadDispatchingTable(); err != nil {
				slog.Error("Unable to query", "table", dispatchTable, "err", err)
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return items, true
}

func (db *DB) CreateDispatchingTable() bool {
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

func (db *DB) CreateDispatchingIndex() bool {
	indexes := []Index{
		Index{"idxdisp_rank", "UNIQUE", dispatchTable, "rank"},
		Index{"idxdisp_dst_table", "UNIQUE", dispatchTable, "dst_table"},
	}
	return db.CreateIndexes(indexes)
}

func (db *DB) ConsolidateData(items []Item) bool {
	now := time.Now()
	for _, item := range items {
		if maxts, ok := db.ReadMaxTimestamp(item.dst); ok {
			lastBrowsed[item.dst] = int64((int64(maxts)/item.period)+1) * item.period
		} else {
			lastBrowsed[item.dst] = 0
		}
		item.clist = mapNames(item.aggr, []string{}, "v%s")
		item.dlist = mapNames(item.aggr, []string{}, "v%s DOUBLE NOT NULL")
		idx := slices.IndexFunc(items, func(i Item) bool { return i.dst == item.src })
		if idx >= 0 {
			item.alist = mapNames(item.aggr, items[idx].aggr, "%%s(v%s)")
		} else {
			item.alist = mapNames(item.aggr, []string{defaultCol, defaultCol, defaultCol, defaultCol}, "%%s(%s)")
		}
		t2 := int64(int64((now.Unix()-margeTemps))/item.period) * item.period
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
			"ts", t2,
			"received", measReceived[item.src])
		if db.BeginTransaction() {
			if !db.InsertConsolidatedData(item, t1, t2) {
				db.RollbackTransaction()
				continue
			}
			if item.retention > 0 && t2 > item.retention {
				if !db.DeleteData(item.dst, 0, t2-item.retention) {
					db.RollbackTransaction()
					continue
				}
			}
			if item.src_delete == "yes" {
				if !db.DeleteData(item.src, t1, t2) {
					db.RollbackTransaction()
					continue
				}
				measReceived[item.src] = 0
			}
			db.CommitTransaction()
		}
	}

	slog.Info("Consolidated", "now", now)
	return true
}

func (db *DB) CreateMeasurementTable(table string) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts DOUBLE NOT NULL,
		sensorid TINYTEXT NOT NULL,
		%s DOUBLE NOT NULL,
		name TINYTEXT,
		place TINYTEXT
	);
	`
	cmd := fmt.Sprintf(cmdTemplate, table, defaultCol)
	if _, err := db.Exec(cmd); err != nil {
		slog.Error("Unable to create", "table", table, "cmd", cmd, "err", err)
		return false
	}

	slog.Info("Table created", "table", table)
	return true
}

func (db *DB) CreateMeasurementIndex(table string) bool {
	indexes := []Index{
		Index{"idxmeas_ts_sensorid", "", table, "ts, sensorid"},
		Index{"idxmeas_ts", "", table, "ts"},
	}
	return db.CreateIndexes(indexes)
}

func (db *DB) CreateConsolidatedTable(item Item) bool {
	cmdTemplate := `
	CREATE TABLE IF NOT EXISTS %s (
		ts INT NOT NULL,
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

func (db *DB) CreateConsolidatedIndex(table string) bool {
	indexes := []Index{
		Index{"idxcons_ts_sensorid_name_place", "UNIQUE", table, "ts, sensorid, name, place"},
		Index{"idxcons_ts", "", table, "ts"},
	}
	return db.CreateIndexes(indexes)
}

func (db *DB) ReadDispatchingTable() ([]Item, error) {
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
			item.retention *= 3600
			result = append(result, item)
		}
	}
	rows.Close()

	return result, nil
}

func (db *DB) ReadMaxTimestamp(table string) (float64, bool) {
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

func (db *DB) InsertMeasurement(dp *Datapoint) bool {
	cmdTemplate := `
	INSERT INTO %s (ts, sensorid, %s, name, place) values (?, ?, ?, ?, ?);
	`
	table := fmt.Sprintf(measurementTmpl, dp.Measurement)
	cmd := fmt.Sprintf(cmdTemplate, table, defaultCol)
	stmt, err := db.Prepare(cmd)
	if err != nil {
		slog.Warn("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err)
		if db.CreateMeasurementTable(table) && db.CreateMeasurementIndex(table) {
			if stmt, err = db.Prepare(cmd); err != nil {
				slog.Error("Unable to prepare stmt", "table", table, "cmd", cmd, "err", err)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	result, err := stmt.Exec(float64(dp.Timestamp)/1000.0, dp.Tags.ID, dp.Fields.Value, dp.Tags.Name, dp.Tags.Place)
	if err != nil {
		slog.Error("Insert error", "table", table, "data", dp, "err", err)
		return false
	}

	affected, _ := result.RowsAffected()
	slog.Debug("Inserted", "data", dp, "affected rows", affected)
	measReceived[table] += affected

	return true
}

func (db *DB) InsertConsolidatedData(item Item, t1 int64, t2 int64) bool {

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
		if db.CreateConsolidatedTable(item) && db.CreateConsolidatedIndex(item.dst) {
			if stmt, err = db.Prepare(cmd); err != nil {
				slog.Error("Unable to prepare stmt", "table", item.dst, "cmd", cmd, "err", err)
				return false
			}
		} else {
			return false
		}
	}
	defer stmt.Close()

	result, err2 := stmt.Exec()
	if err2 != nil {
		slog.Error("Insert error", "table", item.dst, "err", err2)
		return false
	}

	affected, _ := result.RowsAffected()
	slog.Info("Inserted", "table", item.dst, "t1", t1, "t2", t2, "affected rows", affected)

	return true
}

func (db *DB) DeleteData(table string, t1 int64, t2 int64) bool {
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

	result, err := stmt.Exec()
	if err != nil {
		slog.Error("Delete error", "table", table, "err", err)
		return false
	}

	affected, _ := result.RowsAffected()
	slog.Info("Deleted", "table", table, "t1", t1, "t2", t2, "affected rows", affected)

	return true
}

func (db *DB) BeginTransaction() bool {
	if _, err := db.Exec("START TRANSACTION"); err != nil {
		slog.Error("Unable to start a transaction", "err", err)
		return false
	}
	return true
}

func (db *DB) CommitTransaction() bool {
	if _, err := db.Exec("COMMIT"); err != nil {
		slog.Error("Unable to commit a transaction", "err", err)
		return false
	}
	return true
}

func (db *DB) RollbackTransaction() bool {
	if _, err := db.Exec("ROLLBACK"); err != nil {
		slog.Error("Unable to rollback a transaction", "err", err)
		return false
	}
	return true
}

func (db *DB) CreateIndexes(indexes []Index) bool {
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
