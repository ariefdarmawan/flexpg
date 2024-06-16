module github.com/ariefdarmawan/flexpg

go 1.16

replace git.kanosolution.net/kano/dbflex => ../dbflex

require (
	git.kanosolution.net/kano/dbflex v1.3.2
	github.com/ariefdarmawan/serde v0.1.1
	github.com/lib/pq v1.10.7
	github.com/sebarcode/codekit v0.1.4
	github.com/sebarcode/logger v0.1.1
	github.com/smartystreets/goconvey v1.7.2
)
