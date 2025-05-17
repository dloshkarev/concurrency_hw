package compute

type CommandId int8

type CommandSettings struct {
	id       CommandId
	argCount int
}

const (
	SetCommandToken = "SET"
	GetCommandToken = "GET"
	DelCommandToken = "DEL"

	SetCommandId = CommandId(1)
	GetCommandId = CommandId(2)
	DelCommandId = CommandId(3)
)

var commandSettings = map[string]CommandSettings{
	SetCommandToken: {id: SetCommandId, argCount: 2},
	GetCommandToken: {id: GetCommandId, argCount: 1},
	DelCommandToken: {id: DelCommandId, argCount: 1},
}
