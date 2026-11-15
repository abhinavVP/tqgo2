package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"scheduled-pipeline/internal/repository"
	"scheduled-pipeline/internal/tasks"
)

type Server struct {
	repository repository.TaskRepository
	now        func() time.Time
}

func New(repository repository.TaskRepository) *Server {
	return &Server{repository: repository, now: time.Now}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/tasks", s.createTask)
	return mux
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var request tasks.CreateTaskDto
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return
	}
	if err := request.Validate(s.now()); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	task := tasks.NewTask(request, s.now())
	if err := s.repository.Create(r.Context(), task); err != nil {
		if errors.Is(err, r.Context().Err()) {
			writeJSON(w, http.StatusRequestTimeout, map[string]string{"error": "request cancelled"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create task"})
		return
	}
	log.Printf("task accepted id=%s type=%s scheduled_at=%s", task.ID, task.TaskType, task.ScheduledAt.Format(time.RFC3339))
	writeJSON(w, http.StatusCreated, task)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
