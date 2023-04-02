package fcm

import (
	"context"
	"encoding/json"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"google.golang.org/api/option"
	"os"
)

func CliCommand() *cli.Command {
	return &cli.Command{
		Name:  "fcm",
		Usage: "fcm usage",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "cred", Aliases: []string{"c"}},
			&cli.StringFlag{Name: "account", Aliases: []string{"a"}},
			&cli.StringFlag{Name: "priority", Aliases: []string{"p"}},
			&cli.BoolFlag{Name: "batch", Aliases: []string{"b"}},
			&cli.BoolFlag{Name: "dry_run"},

			&cli.StringFlag{Name: "messages_file"},

			&cli.BoolFlag{Name: "notification", Aliases: []string{"n"}},
			&cli.StringFlag{Name: "title", Aliases: []string{"T"}},
			&cli.StringFlag{Name: "body", Aliases: []string{"B"}},
			&cli.StringFlag{Name: "tokens", Aliases: []string{"t"}},
			&cli.StringFlag{Name: "tokens_file"},
			&cli.StringFlag{Name: "data", Aliases: []string{"d"}},
			&cli.BoolFlag{Name: "random", Aliases: []string{"r"}},
			&cli.StringFlag{Name: "data_file"},
		},
		Action: main,
	}
}

func getAccount(path string) (string, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	var contents map[string]interface{}
	err = json.Unmarshal(bytes, &contents)
	if err != nil {
		return "", nil
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

func getMessages(ctx *cli.Context) ([]*messaging.Message, error) {
	return nil, nil
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
						log.Error().Err(res.Error).Int("index", numOfTried+i)
					}
				}
			}
		}
	} else {
		SendMethod := fcmClient.Send
		if ctx.Bool("dry_run") {
			SendMethod = fcmClient.SendDryRun
		}
		for _, m := range messages {
			response, err := SendMethod(context.Background(), m)
			if err != nil {
				log.Error().Err(err)
			} else {
				log.Debug().Str("response", response)
			}
		}
	}

	return nil
}
