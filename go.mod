module github.com/restatedev/sdk-go

go 1.21.0

toolchain go1.21.12

require (
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/google/uuid v1.6.0
	github.com/mr-tron/base58 v1.2.0
	github.com/stretchr/testify v1.9.0
	golang.org/x/net v0.23.0
	google.golang.org/protobuf v1.33.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/restatedev/sdk-go/generated/dev/restate => ./generated/dev/restate
