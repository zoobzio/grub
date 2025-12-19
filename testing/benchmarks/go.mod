module github.com/zoobzio/grub/testing/benchmarks

go 1.25.5

require (
	github.com/zoobzio/grub v0.0.0-00010101000000-000000000000
	github.com/zoobzio/grub/testing v0.0.0-00010101000000-000000000000
)

require (
	github.com/zoobzio/capitan v0.0.13 // indirect
	github.com/zoobzio/sentinel v0.0.6 // indirect
)

replace (
	github.com/zoobzio/grub => ../../
	github.com/zoobzio/grub/testing => ../
)
