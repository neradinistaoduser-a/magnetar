package services

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"go.opentelemetry.io/otel"
)

type AuthZService struct {
	key string
}

func NewAuthZService(key string) AuthZService {
	return AuthZService{key: key}
}

func (s AuthZService) Authorize(ctx context.Context, permName string, objKind string, objId string) bool {
	tracer := otel.Tracer("magnetar.AuthorizeService")
	ctx, span := tracer.Start(ctx, "Authorize")
	defer span.End()
	tokenString, ok := ctx.Value("authz-token").(string)
	if !ok {
		log.Println("no token provided")
		return false
	}
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return []byte(s.key), nil
	})
	if err != nil {
		log.Printf("Error parsing token: %v", err)
		return false
	}

	var permissions []string
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if permissionsClaim, ok := claims["permissions"].(string); ok {
			permissions = strings.Split(permissionsClaim, ",")
		} else {
			log.Println("Custom Claim permissions is not a string or does not exist.")
			return false
		}
	} else {
		log.Println("Invalid claims type.")
		return false
	}

	reqPerm := fmt.Sprintf("%s|%s|%s", permName, objKind, objId)
	for _, perm := range permissions {
		if perm == reqPerm {
			return true
		}
	}

	log.Println("required permission not found")
	return false
}
