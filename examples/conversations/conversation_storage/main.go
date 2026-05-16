package main

import (
	"context"
	"fmt"
	"log"
	"os"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/plugins/conversations"
)

type surveyData struct {
	Language string `json:"language"`
	Years    string `json:"years"`
	Project  string `json:"project"`
}

func main() {
	apiID := mustEnv("API_ID")
	apiHash := mustEnv("API_HASH")
	botToken := mustEnv("BOT_TOKEN")

	client, err := tg.NewClient(mustAtoi(apiID), apiHash, &tg.Config{
		BotToken:    botToken,
		SessionName: "survey_bot",
	})
	if err != nil {
		log.Fatalf("new client: %v", err)
	}

	conv := conversations.New()
	client.Use(conv)

	conv.Register("survey", surveyFlow)

	client.OnMessage(func(ctx *tg.Context) {
		ctx.Reply("Welcome! Use /survey to start the developer survey.")
	}, tg.Command("start"))

	client.OnMessage(func(ctx *tg.Context) {
		if conv.Exit(ctx) {
			ctx.Reply("Survey cancelled.")
		}
	}, tg.Command("cancel"))

	client.OnMessage(func(ctx *tg.Context) {
		if err := conv.Enter("survey", ctx); err != nil {
			ctx.Reply(fmt.Sprintf("Error: %v", err))
		}
	}, tg.Command("survey"))

	if err := client.Connect(0); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Stop()

	bot, err := client.GetMe(context.Background())
	if err != nil {
		log.Fatalf("get me: %v", err)
	}

	fmt.Println("=== Survey Bot (SQLite) ===")
	fmt.Printf("  Bot: %s (@%s)\n", bot.FirstName, bot.Username)
	fmt.Println("  Commands: /survey, /cancel")
	fmt.Println("───────────────────────────")
	fmt.Println("bot running, press Ctrl+C to stop")
	client.Idle()
}

func surveyFlow(c *conversations.ConversationContext) error {
	c.SendMessage("What programming language is your favorite? (or /cancel to quit)")
	ctx, err := c.WaitMessage()
	if err != nil {
		return err
	}
	lang := ctx.Message.Text

	c.SendMessage("How many years of experience do you have?")
	ctx, err = c.WaitMessage()
	if err != nil {
		return err
	}
	years := ctx.Message.Text

	c.SendMessage("What project are you most proud of?")
	ctx, err = c.WaitMessage()
	if err != nil {
		return err
	}
	project := ctx.Message.Text

	c.SetData(&surveyData{
		Language: lang,
		Years:    years,
		Project:  project,
	})

	c.SendMessage(fmt.Sprintf(
		"Thanks! Here's your survey:\n\n"+
			"Language: %s\n"+
			"Experience: %s years\n"+
			"Project: %s",
		lang, years, project,
	))

	return nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("environment variable %s is required", key)
	}
	return v
}

func mustAtoi(s string) int32 {
	var n int32
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		log.Fatalf("invalid integer %q: %v", s, err)
	}
	return n
}
