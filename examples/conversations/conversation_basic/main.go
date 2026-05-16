package main

import (
	"context"
	"fmt"
	"log"
	"os"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/plugins/conversations"
)

func main() {
	apiID := mustEnv("API_ID")
	apiHash := mustEnv("API_HASH")
	botToken := mustEnv("BOT_TOKEN")

	client, err := tg.NewClient(mustAtoi(apiID), apiHash, &tg.Config{
		BotToken:    botToken,
		SessionName: "conv_bot",
		InMemory:    true,
	})
	if err != nil {
		log.Fatalf("new client: %v", err)
	}

	conv := conversations.New()
	client.Use(conv)

	conv.Register("signup", signupFlow)

	client.OnMessage(func(ctx *tg.Context) {
		ctx.Reply("Use /signup to start registration, or /cancel to abort.")
	}, tg.Command("start"))

	client.OnMessage(func(ctx *tg.Context) {
		if conv.Exit(ctx) {
			ctx.Reply("Conversation cancelled.")
		}
	}, tg.Command("cancel"))

	if err := client.Connect(0); err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Stop()

	bot, err := client.GetMe(context.Background())
	if err != nil {
		log.Fatalf("get me: %v", err)
	}

	fmt.Println("=== Conversations Bot ===")
	fmt.Printf("  Bot: %s (@%s)\n", bot.FirstName, bot.Username)
	fmt.Println("  Commands: /signup, /cancel")
	fmt.Println("─────────────────────────")
	fmt.Println("bot running, press Ctrl+C to stop")
	client.Idle()
}

func signupFlow(c *conversations.ConversationContext) error {
	c.SendMessage("What's your first name?")
	ctx, err := c.WaitMessage()
	if err != nil {
		return err
	}
	firstName := ctx.Message.Text

	c.SendMessage("What's your last name?")
	ctx, err = c.WaitMessage()
	if err != nil {
		return err
	}
	lastName := ctx.Message.Text

	c.SendMessage(fmt.Sprintf("Nice to meet you, %s %s!", firstName, lastName))
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
