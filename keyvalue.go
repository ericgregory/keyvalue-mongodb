// Mostly copied from https://github.com/wrpc/wrpc/blob/main/examples/go/keyvalue-server/cmd/keyvalue-mem-nats/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	// Go provider SDK and tooling
	"go.opentelemetry.io/otel/trace"
	"go.wasmcloud.dev/provider"
	wrpc "wrpc.io/go"

	// Generated WIT bindings
	"mongodb/bindings/exports/wrpc/keyvalue/store"

	// MongoDB drivers
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	errNoSuchStore     = store.NewErrorNoSuchStore()
	errInvalidDataType = store.NewErrorOther("invalid data type stored in map")
)

type Provider struct {
	sourceLinks map[string]provider.InterfaceLinkDefinition
	targetLinks map[string]provider.InterfaceLinkDefinition
	tracer      trace.Tracer
}

type Pair struct {
	Store string `bson:"store,omitempty"`
	Key   string `bson:"key,omitempty"`
	Value string `bson:"value,omitempty"`
}

func Ok[T any](v T) *wrpc.Result[T, store.Error] {
	return wrpc.Ok[store.Error](v)
}

func (p *Provider) Delete(ctx context.Context, bucket string, key string) (*wrpc.Result[struct{}, store.Error], error) {
	_, span := p.tracer.Start(ctx, "Delete")
	defer span.End()

	log.Printf("At delete step, using config %s\n", globalConfigName)
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(globalUri).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(opts)
	if err != nil {
		return wrpc.Err[struct{}](*errNoSuchStore), nil
	}

	coll := client.Database(globalDb).Collection(bucket)
	filter := bson.D{{Key: "key", Value: key}}

	// Deletes the first document with the named key
	result, err := coll.DeleteOne(context.TODO(), filter)
	// Prints the number of deleted documents
	fmt.Printf("Documents deleted: %d\n", result.DeletedCount)

	// Errors if any errors occur during the operation
	if err != nil {
		// TODO adapt error
		panic(err)
	}
	return Ok(struct{}{}), nil
}

func (p *Provider) Get(ctx context.Context, bucket string, key string) (*wrpc.Result[[]uint8, store.Error], error) {
	_, span := p.tracer.Start(ctx, "Get")
	defer span.End()

	log.Printf("At Get step, using config %s\n", globalConfigName)
	// Use the SetServerAPIOptions() method to set the version of the Stable API on the client
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(globalUri).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(opts)
	if err != nil {
		// TODO adapt error
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			// TODO adapt error
			panic(err)
		}
	}()

	coll := client.Database(globalDb).Collection(bucket)

	filter := bson.D{{Key: "key", Value: key}}

	// Begin FindOne
	// Retrieves the first matching document
	var result bson.M
	options := options.FindOne().SetProjection(bson.D{{"value", 1}, {"_id", 0}})
	err = coll.FindOne(context.TODO(), filter, options).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return wrpc.Err[[]uint8](*errInvalidDataType), nil
		}
		// TODO adapt error
		panic(err)
	}

	output, err := json.Marshal(result)

	// Turn around and unmarshal into a Go type to access the field
	var pair Pair
	json.Unmarshal(output, &pair)

	if err != nil {
		// TODO adapt error
		panic(err)
	}

	buf := []byte(pair.Value)

	return Ok(buf), nil
}

func (p *Provider) Set(ctx context.Context, bucket string, key string, value []byte) (*wrpc.Result[struct{}, store.Error], error) {
	_, span := p.tracer.Start(ctx, "Set")
	defer span.End()

	log.Printf("At Set step, using URI config %s\n", globalUri)
	// Use the SetServerAPIOptions() method to set the version of the Stable API on the client
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(globalUri).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(opts)
	if err != nil {
		// TODO adapt error
		panic(err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			// TODO adapt error
			panic(err)
		}
	}()

	// TODO: erroring for this method

	// Sets the upsert option to true
	options := options.UpdateOne().SetUpsert(true)
	// Sets the collection
	coll := client.Database(globalDb).Collection(bucket)

	store := globalKv
	strvalue := string(value)

	filter := bson.D{{"key", key}, {"store", store}}
	update := bson.D{{"$set", bson.D{{"key", key}, {"store", store}, {"value", strvalue}}}}

	coll.UpdateOne(context.TODO(), filter, update, options)

	return Ok(struct{}{}), nil
}

func (p *Provider) Exists(ctx context.Context, bucket string, key string) (*wrpc.Result[bool, store.Error], error) {
	_, span := p.tracer.Start(ctx, "Exists")
	defer span.End()

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(globalUri).SetServerAPIOptions(serverAPI)
	client, err := mongo.Connect(opts)
	if err != nil {
		return wrpc.Err[bool](*errNoSuchStore), nil
	}

	// Set collection
	coll := client.Database(globalDb).Collection(bucket)

	filter := bson.D{{Key: "key", Value: key}, {Key: "store", Value: globalKv}}

	// Begin FindOne
	// Retrieves the first matching document
	err = coll.FindOne(context.TODO(), filter).Err()

	// Prints a message if no documents are matched or if any
	// other errors occur during the operation
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// TK update error
			return wrpc.Err[bool](*errInvalidDataType), nil
		}
		panic(err)
	}
	response := true
	return Ok(response), nil
}

func (p *Provider) ListKeys(ctx context.Context, bucket string, cur *uint64) (*wrpc.Result[store.KeyResponse, store.Error], error) {
	_, span := p.tracer.Start(ctx, "ListKeys")
	defer span.End()

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(globalUri).SetServerAPIOptions(serverAPI)
	client, clientErr := mongo.Connect(opts)
	if clientErr != nil {
		return wrpc.Err[store.KeyResponse](*errNoSuchStore), nil
	}

	// Set collection
	coll := client.Database(globalDb).Collection(bucket)
	filter := bson.D{{Key: "store", Value: globalKv}}
	options := options.Find().SetProjection(bson.D{{"key", 1}})

	// Retrieves documents that match the query filter
	cursor, err := coll.Find(context.TODO(), filter, options)
	if err != nil {
		panic(err)
	}

	// Unpacks the cursor into a slice
	var keys []string
	for cursor.Next(context.TODO()) {
		// A new result variable should be declared for each document.
		var result Pair
		if err := cursor.Decode(&result); err != nil {
			// TODO: Adapt error handling
			log.Panic(err)
		}
		strresult := fmt.Sprintln(result)
		keys = append(keys, strresult)
	}

	return Ok(store.KeyResponse{
		Keys:   keys,
		Cursor: nil,
	}), nil
}
