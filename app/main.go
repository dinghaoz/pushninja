package main

import (
	"github.com/dinghaoz/pushninja/fcm"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Starting app")
	app := &cli.App{
		Name:        "pushninjia",
		HelpName:    "pushninjia",
		Description: "Push debugging utility",
	}

	app.Commands = []*cli.Command{
		fcm.CliCommand(),
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error().Err(err).Msg("Failed to start app")
	}
}
