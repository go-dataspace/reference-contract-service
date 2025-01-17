// Copyright 2025 go-dataspace
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package contract

import (
	"io"
	"io/fs"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/gabriel-vasile/mimetype"
	"github.com/google/uuid"
)

// publishedFile represents a published file.
type publishedFile struct {
	ID             uuid.UUID
	File           *fileInfo
	Token          string
	PathIdentifier string
}

// publishRegistry is a small structure to keep track of published files.
type publishRegistry struct {
	byUUID   map[uuid.UUID]*publishedFile
	byPathID map[string]*publishedFile
	sync.RWMutex
}

func newPublishRegistry() *publishRegistry {
	return &publishRegistry{
		byUUID:   make(map[uuid.UUID]*publishedFile),
		byPathID: make(map[string]*publishedFile),
	}
}

func (p *publishRegistry) GetByUUID(id uuid.UUID) *publishedFile {
	defer p.RUnlock()
	p.RLock()
	return p.byUUID[id]
}

func (p *publishRegistry) GetByPathID(id string) *publishedFile {
	defer p.RUnlock()
	p.RLock()
	return p.byPathID[id]
}

func (p *publishRegistry) Put(pf *publishedFile) {
	defer p.Unlock()
	p.Lock()
	p.byUUID[pf.ID] = pf
	p.byPathID[pf.PathIdentifier] = pf
}

func (p *publishRegistry) Del(id uuid.UUID) {
	defer p.Unlock()
	p.Lock()
	if pf, ok := p.byUUID[id]; ok {
		delete(p.byPathID, pf.PathIdentifier)
		delete(p.byUUID, pf.ID)
	}
}

// Mux returns a mux that is ready to serve the files.
func (s *Server) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{path...}", s.ServeHTTP)
	return mux
}

// ServeHTTP is the handle function to serve the files.
// It will look up file offer and serve it if the token was in the header.
// Note that it will always return a 404 to make guessing the path harder.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	token := extractTokenFromRequest(req)
	if token == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	reqPath := req.PathValue("path")
	pathParts := strings.Split(reqPath, "/")
	if len(pathParts) != 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fo := s.registry.GetByPathID(pathParts[0])
	if fo == nil || path.Base(fo.File.FullPath) != pathParts[1] || fo.Token != token {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	s.serveFile(w, fo.File)
}

// serveFile serves up the file, and as authentication has succeeded at this point
// we will return non-404 errors.
func (s *Server) serveFile(w http.ResponseWriter, fp *fileInfo) {
	finfo, err := fp.DirEntry.Info()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	mtype, err := detectMIMEType(s.dir, fp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fh, err := s.dir.Open(fp.FullPath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Del("Content-Type")
	w.Header().Add("Content-Type", mtype.String())
	w.Header().Add("Content-Length", strconv.Itoa(int(finfo.Size())))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, fh)
	if err != nil {
		return
	}
}

func detectMIMEType(dir fs.FS, fp *fileInfo) (*mimetype.MIME, error) {
	fh, err := dir.Open(fp.FullPath)
	if err != nil {
		return nil, err
	}
	mimeType, err := mimetype.DetectReader(fh)
	if err != nil {
		return nil, err
	}
	fh.Close()
	return mimeType, nil
}

func extractTokenFromRequest(req *http.Request) string {
	headerVal := req.Header.Get("Authorization")
	parts := strings.Split(headerVal, " ")
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return parts[1]
}
