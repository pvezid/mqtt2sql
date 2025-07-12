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
	"encoding/json"
	"log/slog"
)

func JSONHandler(ich <-chan string) chan Datapoint {

	c := make(chan Datapoint, 10)

	go func() {
		defer close(c)
		for msg := range ich {
			var dps []Datapoint
			slog.Debug("String received", "msg", msg)
			err := json.Unmarshal([]byte(msg), &dps)
			if err != nil {
				slog.Error("Unmarshal", "error", err)
			} else {
				for _, dp := range dps {
					c <- dp
				}
			}
		}
	}()

	return c
}
