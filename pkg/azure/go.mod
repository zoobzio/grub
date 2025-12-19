module github.com/zoobzio/grub/pkg/azure

go 1.25.5

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.5.0
	github.com/zoobzio/grub v0.0.0-00010101000000-000000000000
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.16.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.10.0 // indirect
	github.com/zoobzio/capitan v0.0.13 // indirect
	github.com/zoobzio/sentinel v0.0.6 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/text v0.19.0 // indirect
)

replace github.com/zoobzio/grub => ../../
