package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"

	"github.com/mirror520/events"
	"github.com/mirror520/events/model"
)

func StoreHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var request events.StoreRequest
		if err := ctx.ShouldBind(&request); err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusBadRequest, result)
			return
		}

		_, err := endpoint(ctx, request)
		if err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("event stored")
		ctx.JSON(http.StatusOK, result)
	}
}

func ReplayHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var request events.ReplayRequest

		if fromStr := ctx.Query("from"); fromStr != "" {
			from, err := time.Parse(time.RFC3339Nano, fromStr)
			if err != nil {
				result := model.FailureResult(err)
				ctx.AbortWithStatusJSON(http.StatusBadRequest, result)
				return
			}

			request.From = from
		}

		if topicStr := ctx.Query("topic"); topicStr != "" {
			request.Topics = strings.Split(topicStr, ",")
		}

		_, err := endpoint(ctx, request)
		if err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("ok")
		ctx.JSON(http.StatusOK, result)
	}
}
