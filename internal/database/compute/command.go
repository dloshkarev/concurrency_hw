package compute

type CommandId int8

const (
	SetCommandId     = CommandId(1)
	GetCommandId     = CommandId(2)
	DelCommandId     = CommandId(3)
	UnknownCommandId = CommandId(-1)
)

const (
	SetCommandToken = "SET"
	GetCommandToken = "GET"
	DelCommandToken = "DEL"
)

var commandMapping = map[string]CommandId{
	SetCommandToken: SetCommandId,
	GetCommandToken: GetCommandId,
	DelCommandToken: DelCommandId,
}
