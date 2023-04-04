package fcm

import (
	"context"
	"encoding/csv"
	"encoding/json"
	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"google.golang.org/api/option"
	"io"
	"os"
	"strconv"
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

			&cli.PathFlag{Name: "targets"},
			&cli.BoolFlag{Name: "targets_has_header", Value: false},
			&cli.StringFlag{Name: "targets_token_col", Value: "0"},
			&cli.StringFlag{Name: "targets_id_col", Value: "0"},

			&cli.BoolFlag{Name: "random", Aliases: []string{"r"}},
			&cli.PathFlag{Name: "data_file"},
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

type target struct {
	Id    string
	Token string
}

func getTargets(path string, hasHeader bool, idCol string, tokenCol string) ([]target, error) {
	log.Debug().Msg("getTargets")
	if len(path) == 0 {
		return nil, fmt.Errorf("missing target file name")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error().Err(err).Msg("Failed to close file")
		}
	}(file)

	var targets []target
	var header []string
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if header == nil && hasHeader {
			header = record
		} else {
			var t target
			for i, field := range record {
				col := strconv.Itoa(i)
				if header != nil {
					col = header[i]
				}

				if col == idCol {
					t.Id = field
				} else if col == tokenCol {
					t.Token = field
				}
			}

			targets = append(targets, t)
		}
	}

	return targets, nil
}

func getMessages(targets []target, ctx *cli.Context) ([]*messaging.Message, error) {
	log.Debug().Msg("getMessages")
	if len(targets) == 0 {
		return nil, fmt.Errorf("missing tokens")
	}

	log.Debug().Int("len", len(targets)).Msg("convert from targets")

	var data map[string]string = nil
	dataFile := ctx.Path("data_file")
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
	for _, t := range targets {
		if len(t.Token) == 0 {
			continue
		}

		message := &messaging.Message{
			Token: t.Token,
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

	targets, err := getTargets(
		ctx.Path("targets"),
		ctx.Bool("targets_has_header"),
		ctx.String("targets_id_col"),
		ctx.String("targets_token_col"),
	)
	if err != nil {
		return err
	}

	log.Info().Int("num", len(targets)).Msg("targets retrieved")

	messages, err := getMessages(targets, ctx)
	if err != nil {
		return err
	}

	var failedIds []string
	var succeededIds []string

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
				log.Error().Err(err).Int("fromIndex", numOfTried).Msg("Failed to send")
				for i := numOfTried; i < numOfTried+numToSend; i++ {
					failedIds = append(failedIds, targets[i].Id)
				}
			} else {
				for i, res := range response.Responses {
					index := numOfTried + i
					if !res.Success {
						log.Error().Err(res.Error).Int("index", index).Str("Id", targets[index].Id).Msg("Failed response")
						failedIds = append(failedIds, targets[index].Id)
					} else {
						succeededIds = append(succeededIds, targets[index].Id)
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
		for i, m := range messages {
			response, err := SendMethod(context.Background(), m)
			if err != nil {
				log.Error().Err(err).Int("index", i).Str("Id", targets[i].Id).Msg("Failed response")
				failedIds = append(failedIds, targets[i].Id)
			} else {
				log.Debug().Str("response", response).Msg("msg sent")
				succeededIds = append(succeededIds, targets[i].Id)
			}
		}
	}

	log.Info().Int("Succeeded", len(succeededIds)).Int("Failed", len(failedIds)).Msg("Results")
	log.Info().Interface("SucceededIds", succeededIds).Msg("Details")

	return nil
}
