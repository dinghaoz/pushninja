package fcm

import (
	"context"
	"encoding/json"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"google.golang.org/api/option"
	"os"
	"strings"
	"time"
)

func CliCommand() *cli.Command {
	return &cli.Command{
		Name:  "fcm",
		Usage: "fcm usage",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "cred", Aliases: []string{"C"}, Required: true},
			&cli.StringFlag{Name: "account", Aliases: []string{"a"}},
			&cli.StringFlag{Name: "priority", Aliases: []string{"p"}, Value: "normal"},
			&cli.StringFlag{Name: "channel", Aliases: []string{"c"}},
			&cli.IntFlag{Name: "ttl", Value: 3600},
			&cli.BoolFlag{Name: "batch", Aliases: []string{"b"}},
			&cli.BoolFlag{Name: "dry_run"},

			&cli.BoolFlag{Name: "notification", Aliases: []string{"n"}},
			&cli.StringFlag{Name: "title", Aliases: []string{"T"}},
			&cli.StringFlag{Name: "body", Aliases: []string{"B"}},
			&cli.StringFlag{Name: "tokens", Aliases: []string{"t"}},
			&cli.StringFlag{Name: "tokens_file"},
			&cli.BoolFlag{Name: "random", Aliases: []string{"r"}},
			&cli.StringFlag{Name: "data_file"},
		},
		Action: main,
	}
}

func toJsonMap(path string) (map[string]interface{}, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var contents map[string]interface{}
	err = json.Unmarshal(bytes, &contents)
	if err != nil {
		return nil, err
	}

	return contents, nil
}

func getAccount(path string) (string, error) {
	contents, err := toJsonMap(path)
	if err != nil {
		return "", err
	}
	if clientEmail, found := contents["client_email"]; found {
		if account, ok := clientEmail.(string); !ok {
			return "", fmt.Errorf("client_email is not a string")
		} else {
			return account, nil
		}
	} else {
		return "", fmt.Errorf("missing client_email")
	}
}

func stringifyJson(m map[string]interface{}) map[string]string {
	data := make(map[string]string)
	for k, v := range m {
		if s, ok := v.(string); ok {
			data[k] = s
		} else {
			data[k] = fmt.Sprintf("%v", v)
		}
	}
	return data
}

func randomizeIfNeeded(data map[string]string, random bool) map[string]string {
	if !random {
		return data
	}
	newData := make(map[string]string, len(data))
	for id, value := range data {
		newData[id] = value
	}

	newData["salt"] = uuid.New().String()

	return newData
}

func getMessages(ctx *cli.Context) ([]*messaging.Message, error) {
	tokens := strings.Split(ctx.String("tokens"), ",")

	tokenFile := ctx.String("tokens_file")
	if len(tokenFile) > 0 {
		bytes, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, strings.Split(string(bytes), "\n")...)
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("missing tokens")
	}

	var data map[string]string = nil
	dataFile := ctx.String("data_file")
	if len(dataFile) > 0 {
		rawData, err := toJsonMap(dataFile)
		if err != nil {
			return nil, err
		}
		data = stringifyJson(rawData)
	}

	var notification *messaging.AndroidNotification = nil
	if ctx.Bool("notification") {
		notification = &messaging.AndroidNotification{
			Title:     ctx.String("title"),
			Body:      ctx.String("body"),
			ChannelID: ctx.String("channel"),
		}
	}

	log.Debug().Int("ttl", ctx.Int("ttl")).Msg("flags")
	ttl := time.Duration(ctx.Int("ttl")) * time.Second

	var messages []*messaging.Message
	for _, token := range tokens {
		if len(token) == 0 {
			continue
		}

		message := &messaging.Message{
			Token: token,
			Android: &messaging.AndroidConfig{
				TTL:          &ttl,
				Priority:     ctx.String("priority"),
				Notification: notification,
				Data:         randomizeIfNeeded(data, ctx.Bool("random")),
			},
		}

		messages = append(messages, message)
	}

	return messages, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main(ctx *cli.Context) error {
	account, err := getAccount(ctx.String("cred"))
	if err != nil {
		return err
	}
	opt := option.WithCredentialsFile(ctx.String("cred"))
	cfg := &firebase.Config{ServiceAccountID: account}
	fbApp, err := firebase.NewApp(context.Background(), cfg, opt)
	if err != nil {
		return err
	}

	fcmClient, err := fbApp.Messaging(context.Background())
	if err != nil {
		return err
	}

	messages, err := getMessages(ctx)

	if ctx.Bool("batch") {
		log.Info().Int("#msg", len(messages)).Msg("BatchSend")
		SendMethod := fcmClient.SendAll
		if ctx.Bool("dry_run") {
			SendMethod = fcmClient.SendAllDryRun
		}

		var numOfTried = 0
		for {
			if numOfTried == len(messages) {
				break
			}

			numToSend := min(len(messages)-numOfTried, 500)

			response, err := SendMethod(context.Background(), messages[numOfTried:numOfTried+numToSend])
			if err != nil {
				log.Error().Err(err).Int("fromIndex", numOfTried)
			} else {
				for i, res := range response.Responses {
					if !res.Success {
						log.Error().Err(res.Error).Int("index", numOfTried+i).Msg("Failed to send")
					}
				}
			}
			numOfTried += numToSend
		}
	} else {
		log.Info().Int("#msg", len(messages)).Bool("dry_run", ctx.Bool("dry_run")).Msg("Send")
		SendMethod := fcmClient.Send
		if ctx.Bool("dry_run") {
			SendMethod = fcmClient.SendDryRun
		}
		for _, m := range messages {
			response, err := SendMethod(context.Background(), m)
			if err != nil {
				log.Error().Err(err).Msg("Failed to send")
			} else {
				log.Debug().Str("response", response).Msg("msg sent")
			}
		}
	}

	return nil
}
