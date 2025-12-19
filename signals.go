package grub

import "github.com/zoobzio/capitan"

// Signals for CRUD lifecycle events.
var (
	GetStarted      = capitan.NewSignal("grub.get.started", "Record fetch initiated")
	GetCompleted    = capitan.NewSignal("grub.get.completed", "Record fetch succeeded")
	GetFailed       = capitan.NewSignal("grub.get.failed", "Record fetch failed")
	SetStarted      = capitan.NewSignal("grub.set.started", "Record write initiated")
	SetCompleted    = capitan.NewSignal("grub.set.completed", "Record write succeeded")
	SetFailed       = capitan.NewSignal("grub.set.failed", "Record write failed")
	DeleteStarted   = capitan.NewSignal("grub.delete.started", "Record deletion initiated")
	DeleteCompleted = capitan.NewSignal("grub.delete.completed", "Record deletion succeeded")
	DeleteFailed    = capitan.NewSignal("grub.delete.failed", "Record deletion failed")
	ExistsCompleted = capitan.NewSignal("grub.exists.completed", "Record existence checked")
	ListCompleted   = capitan.NewSignal("grub.list.completed", "Record listing completed")
	CountCompleted  = capitan.NewSignal("grub.count.completed", "Record count completed")
)

// Field keys for event extraction.
var (
	FieldKey      = capitan.NewStringKey("key")
	FieldDuration = capitan.NewDurationKey("duration")
	FieldError    = capitan.NewErrorKey("error")
	FieldExists   = capitan.NewBoolKey("exists")
	FieldCount    = capitan.NewInt64Key("count")
	FieldCursor   = capitan.NewStringKey("cursor")
	FieldLimit    = capitan.NewIntKey("limit")
	FieldKeys     = capitan.NewKey[[]string]("keys", "grub.Keys")
)
