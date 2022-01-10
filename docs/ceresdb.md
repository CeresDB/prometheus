# PromQL compatibility
- metric name is required
- `at` syntax not supported
- when `max`, CeresDB will return `NaN` if contains `NaN`, Prometheus will filter `NaN` out
- `9.988465674311579e+307` will be evaled to `Inf`, `-9.988465674311579e+307` will be evaled to `-Inf` in CeresDB.
