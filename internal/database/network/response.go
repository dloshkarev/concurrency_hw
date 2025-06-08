package network

const (
	NoConnectionsAvailable  = "[error] no connections available"
	CannotParseQuery        = "[error] cannot parse query"
	UnknownCommand          = "[error] unknown command: %v"
	CommandStoreError       = "[error] command storing failed: %v"
	CommandReplicationError = "[error] modifying command cannot be executed on slave: %v"
	SlaveReplicationError   = "[error] slave cannot handle replication request"
	SuccessCommand          = "[success]"
	GetResult               = "[success] %v"
)
