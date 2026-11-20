| scenario             | committed | lost | ghosts | double-applied | duplicates delivered | hash mismatch | gates |
| -------------------- | --------- | ---- | ------ | -------------- | -------------------- | ------------- | ----- |
| after-commit+naive   | 1316      | 274  | 0      | 2              | 2                    | 0             | ok    |
| in-transaction+naive | 1049      | 0    | 25     | 2              | 2                    | 0             | ok    |
| outbox+naive         | 1412      | 0    | 0      | 25             | 25                   | 0             | ok    |
| outbox+inbox         | 1415      | 0    | 0      | 0              | 25                   | 0             | ok    |

Seeds of the last row, same load and chaos recipe (n = 3). Seed 42 is the last row of the table above, the others come from `mise run scenario -- outbox+inbox --seed N`. The seed fixes the chaos schedule and the crash draws, not the timing, so the duplicate counts differ.

| seed | committed | lost | ghosts | double-applied | duplicates delivered | hash mismatch | gates |
| ---- | --------- | ---- | ------ | -------------- | -------------------- | ------------- | ----- |
| 42   | 1415      | 0    | 0      | 0              | 25                   | 0             | ok    |
| 43   | 1107      | 0    | 0      | 0              | 9                    | 0             | ok    |
| 44   | 1691      | 0    | 0      | 0              | 29                   | 0             | ok    |
