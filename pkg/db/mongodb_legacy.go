package db

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/globalsign/mgo"
)

// MongoDBLegacy represents a legacy MongoDB connection using mgo driver
type MongoDBLegacy struct {
	session  *mgo.Session
	database string
}

// NewMongoDBLegacy creates a new legacy MongoDB connection using mgo
func NewMongoDBLegacy(connectionString, database string) (*MongoDBLegacy, error) {
	// Parse connection string and create dial info
	dialInfo, err := mgo.ParseURL(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Set timeout
	dialInfo.Timeout = 10 * time.Second

	// Create session
	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Set mode to eventual for better read performance
	session.SetMode(mgo.Eventual, true)

	// Ping to verify connection
	if err := session.Ping(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &MongoDBLegacy{
		session:  session,
		database: database,
	}, nil
}

// NewMongoDBLegacyWithTLS creates a new legacy MongoDB connection with TLS
func NewMongoDBLegacyWithTLS(connectionString, database string, tlsConfig *tls.Config) (*MongoDBLegacy, error) {
	dialInfo, err := mgo.ParseURL(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	dialInfo.Timeout = 10 * time.Second
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), tlsConfig)
	}

	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	session.SetMode(mgo.Eventual, true)

	if err := session.Ping(); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &MongoDBLegacy{
		session:  session,
		database: database,
	}, nil
}

// GetSession returns the mgo session
func (m *MongoDBLegacy) GetSession() *mgo.Session {
	return m.session.Copy()
}

// GetDatabase returns the database name
func (m *MongoDBLegacy) GetDatabase() string {
	return m.database
}

// GetCollection returns a collection from the database
func (m *MongoDBLegacy) GetCollection(collectionName string) *mgo.Collection {
	session := m.session.Copy()
	return session.DB(m.database).C(collectionName)
}

// Close closes the MongoDB connection
func (m *MongoDBLegacy) Close() {
	if m.session != nil {
		m.session.Close()
	}
}

// ListCollections lists all collection names in the database
func (m *MongoDBLegacy) ListCollections() ([]string, error) {
	session := m.session.Copy()
	defer session.Close()

	names, err := session.DB(m.database).CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	// Filter out system collections
	var collections []string
	for _, name := range names {
		if name != "system.indexes" && name != "system.users" && name != "system.profile" && name != "system.namespaces" {
			collections = append(collections, name)
		}
	}

	return collections, nil
}

// ListIndexes returns all indexes for a collection using the mgo driver
func (m *MongoDBLegacy) ListIndexes(collectionName string) ([]mgo.Index, error) {
	session := m.session.Copy()
	defer session.Close()

	col := session.DB(m.database).C(collectionName)
	indexes, err := col.Indexes()
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes for collection %s: %w", collectionName, err)
	}

	return indexes, nil
}

// Ping tests the connection to MongoDB
func (m *MongoDBLegacy) Ping() error {
	return m.session.Ping()
}
