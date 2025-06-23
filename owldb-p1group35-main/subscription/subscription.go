package subscription

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// SubscriberHandler manages subscriptions and client channels for resources.
type SubscriberHandler struct {
	lock        sync.RWMutex
	clientChans map[string][]chan string
}

// NewHandler initializes a new SubscriberHandler.
// Input: None
// Output: New SubscriberHandler (*SubscriberHandler)
func NewHandler() *SubscriberHandler {
	return &SubscriberHandler{
		clientChans: make(map[string][]chan string),
	}
}

// Register adds a client channel to a resource's subscription list.
// Input: Resource ID (string), Client channel (chan string)
// Output: Error if the channel is already registered
func (h *SubscriberHandler) Register(resourceID string, clientChan chan string) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	slog.Info("Registering channel", "resource", resourceID)

	// Check if the channel is already registered
	if clients, ok := h.clientChans[resourceID]; ok {
		for _, client := range clients {
			if client == clientChan {
				slog.Error("Channel already registered", "resource", resourceID)
				return errors.New("channel already registered")
			}
		}
	}
	// Add the channel to the resource's subscription list
	h.clientChans[resourceID] = append(h.clientChans[resourceID], clientChan)
	slog.Info("Channel registered successfully", "resource", resourceID, "client_count", len(h.clientChans[resourceID]))
	return nil
}

// Dispatch sends event messages to all subscribers of a specific resource.
// Input: Resource ID (string), Event path (string), Event type (string)
// Output: Error if message dispatch fails to some clients
func (h *SubscriberHandler) Dispatch(resourceID string, eventData []byte, sameLevel bool, eventType string) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	slog.Info("Dispatching event", "resource", resourceID)

	subscribers, ok := h.clientChans[resourceID]
	if !ok || len(subscribers) == 0 {
		slog.Warn("No clients to notify", "resource", resourceID)
		return errors.New("no clients to notify")
	}

	// Construct the event message
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("event: %s\n", eventType))
	buffer.WriteString(fmt.Sprintf("data: %s\n", string(eventData)))
	buffer.WriteString(fmt.Sprintf("id: %d\n\n", time.Now().UnixNano()))
	message := buffer.String()

	failed := 0
	// Send the message to all subscribers
	for _, client := range subscribers {
		select {
		case client <- message:
			slog.Info("Message dispatched", "client", client, "message", message)
		default:
			slog.Warn("Failed to dispatch message", "resource", resourceID)
			failed++
		}
	}

	// Clean up channels if the resource is deleted
	if eventType == "delete" && sameLevel {
		delete(h.clientChans, resourceID)
		slog.Info("Resource deleted, channels cleaned", "resource", resourceID)
	}

	if failed > 0 {
		slog.Error("Some channels failed to receive message", "resource", resourceID, "failed_count", failed)
		return errors.New("message dispatch failed to some clients")
	}

	slog.Info("Event dispatched to all clients successfully", "resource", resourceID, "event_type", eventType)
	return nil
}

// HasClients checks if a resource has active subscribers.
// Input: Resource ID (string)
// Output: Boolean indicating if there are active subscribers
func (h *SubscriberHandler) HasClients(resourceID string) bool {
	h.lock.RLock()
	defer h.lock.RUnlock()

	clients, exists := h.clientChans[resourceID]
	slog.Info("in HasClients", "resourceID", resourceID, "clients", clients, "exists", exists)
	return exists && len(clients) > 0
}

// Unregister removes a client channel from a resource's subscription list.
// Input: Resource ID (string), Client channel (chan string)
// Output: None
func (h *SubscriberHandler) Unregister(resourceID string, clientChan chan string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	// Find and remove the client channel from the resource's list
	if clients, ok := h.clientChans[resourceID]; ok {
		for i, client := range clients {
			if client == clientChan {
				h.clientChans[resourceID] = append(clients[:i], clients[i+1:]...)
				slog.Info("Channel unregistered", "resource", resourceID)
				break
			}
		}

		// Clean up if no clients are left for the resource
		if len(h.clientChans[resourceID]) == 0 {
			delete(h.clientChans, resourceID)
			slog.Info("No remaining clients, resource cleaned", "resource", resourceID)
		}
	}
}
