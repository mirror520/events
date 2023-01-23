package events

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"
	"go.uber.org/zap"

	"github.com/mirror520/events/model"
)

func HTTPStoreHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	log := zap.L().With(
		zap.String("transport", "http"),
		zap.String("handler", "store"),
	)

	return func(ctx *gin.Context) {
		var request StoreRequest
		if err := ctx.ShouldBind(&request); err != nil {
			log.Error(err.Error())

			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusBadRequest, result)
			return
		}

		_, err := endpoint(ctx, request)
		if err != nil {
			log.Error(err.Error())

			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("event stored")
		ctx.JSON(http.StatusOK, result)
	}
}
