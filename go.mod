module github.com/spenczar/quaminapb

go 1.23

replace quamina.net/go/quamina/v2 => ../../timbray/quamina

require (
	google.golang.org/protobuf v1.36.11
	quamina.net/go/quamina/v2 v2.0.0-00010101000000-000000000000
)
