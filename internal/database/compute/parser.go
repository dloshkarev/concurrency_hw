package compute

import (
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strings"
)

type QueryParser struct {
	logger *zap.Logger
}

func NewQueryParser(logger *zap.Logger) (*QueryParser, error) {
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	return &QueryParser{logger: logger}, nil
}

func (p *QueryParser) ParseQuery(queryString string) (Query, error) {
	p.logger.Debug("parsing query", zap.String("query", queryString))

	tokens := strings.Fields(queryString)

	if len(tokens) == 0 {
		p.logger.Debug("no tokens found", zap.String("query", queryString))
		return Query{}, errors.New("no tokens found")
	}

	commandToken := tokens[0]
	commandId, err := parseCommandId(commandToken)
	if err != nil {
		p.logger.Debug("error parsing command id", zap.String("query", queryString), zap.Error(err))
		return Query{}, err
	}

	if ((commandId == GetCommandId || commandId == DelCommandId) && len(tokens) != 2) ||
		(commandId == SetCommandId && len(tokens) != 3) {
		p.logger.Debug("invalid count of arguments", zap.String("query", queryString))
		return Query{}, errors.New("invalid count of arguments")
	}

	query, err := mapQuery(tokens, commandId)
	if err != nil {
		p.logger.Debug("error parsing query", zap.String("query", queryString), zap.Error(err))
	}
	return query, nil
}

func parseCommandId(token string) (CommandId, error) {
	if commandId, exists := commandMapping[token]; exists {
		return commandId, nil
	}
	return UnknownCommandId, fmt.Errorf("invalid command token: %s", token)
}

func mapQuery(tokens []string, commandId CommandId) (Query, error) {
	switch commandId {
	case SetCommandId:
		if len(tokens) != 3 {
			return Query{}, errors.New("SET: invalid count of arguments")
		}
		return Query{
			commandId: commandId,
			key:       tokens[1],
			value:     tokens[2],
		}, nil
	case GetCommandId:
		if len(tokens) != 2 {
			return Query{}, errors.New("GET: invalid count of arguments")
		}
		return Query{
			commandId: commandId,
			key:       tokens[1],
		}, nil
	case DelCommandId:
		if len(tokens) != 2 {
			return Query{}, errors.New("DEL: invalid count of arguments")
		}
		return Query{
			commandId: commandId,
			key:       tokens[1],
		}, nil
	default:
		return Query{}, errors.New("unknown commandId")
	}
}
