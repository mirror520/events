package events

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/mirror520/events/model"
)

func EventStoreHandler(endpoint Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var request EventStoreRequest
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
