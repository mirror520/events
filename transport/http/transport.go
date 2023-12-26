package http

import (
	"net/http"
	"strconv"

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

func NewIteratorHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var request events.NewIteratorRequest
		if err := ctx.ShouldBind(&request); err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusBadRequest, result)
			return
		}

		id, err := endpoint(ctx, request)
		if err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("iterator created")
		result.Data = id
		ctx.JSON(http.StatusOK, result)
	}
}

func FetchFromIteratorHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		request := events.FetchFromIteratorRequest{
			ID:    ctx.Param("id"),
			Batch: 100,
		}

		if batchStr := ctx.Query("batch"); batchStr != "" {
			batch, err := strconv.Atoi(batchStr)
			if err != nil {
				result := model.FailureResult(err)
				ctx.AbortWithStatusJSON(http.StatusBadRequest, result)
				return
			}

			request.Batch = batch
		}

		response, err := endpoint(ctx, request)
		if err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("event fetched")
		result.Data = response
		ctx.JSON(http.StatusOK, result)
	}
}

func CloseIteratorHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		id := ctx.Param("id")

		_, err := endpoint(ctx, id)
		if err != nil {
			result := model.FailureResult(err)
			ctx.AbortWithStatusJSON(http.StatusUnprocessableEntity, result)
			return
		}

		result := model.SuccessResult("iterator closed")
		ctx.JSON(http.StatusOK, result)
	}
}
