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
	commandSettings, err := parseCommandSettings(commandToken)
	if err != nil {
		p.logger.Debug("error parsing settings", zap.String("query", queryString), zap.Error(err))
		return Query{}, err
	}

	query, err := mapQuery(tokens[1:], commandSettings)
	if err != nil {
		p.logger.Debug("error parsing query", zap.String("query", queryString), zap.Error(err))
		return Query{}, err
	}
	return query, nil
}

func (p *QueryParser) CleanQuery(queryString string) string {
	return strings.ReplaceAll(queryString, "\n", "")
}

func parseCommandSettings(token string) (CommandSettings, error) {
	if setting, exists := commandSettings[token]; exists {
		return setting, nil
	}
	return CommandSettings{}, fmt.Errorf("invalid command token: %s", token)
}

func mapQuery(args []string, settings CommandSettings) (Query, error) {
	if len(args) != settings.argCount {
		return Query{}, errors.New("invalid count of arguments")
	}
	return Query{CommandId: settings.id, Args: args}, nil
}
