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
	"bufio"
	"log/slog"
	"os"
)

func FileHandler(filename string) chan string {
	c := make(chan string, 10)

	go func() {
		defer close(c)
		file := os.Stdin
		var err error

		if filename != "-" {
			file, err = os.Open(filename)
			if err != nil {
				slog.Error("File open", "filename", filename, "error", err)
				return
			}
			defer file.Close()
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			msg := scanner.Text()
			slog.Debug("File scanner", "payload", msg)
			c <- msg
		}
		if err := scanner.Err(); err != nil {
			slog.Error("File scanner", "error", err)
		}
	}()

	return c
}
