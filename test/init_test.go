package test

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/ego-component/egorm"
	"github.com/gotomicro/ego/core/econf"

	_ "github.com/gotomicro/gorm-to-dm"
)

var (
	EgormDB *egorm.Component
)

func init() {
	_, cf, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(cf), "./docker.toml")
	conf, err := os.ReadFile(root)
	if err != nil {
		log.Fatalf("init exited with read file error: %v", err)
	}

	// 加载配置
	err = econf.LoadFromReader(strings.NewReader(string(conf)), toml.Unmarshal)
	if err != nil {
		log.Fatalf("init exited with error: %v", err)
	}

	EgormDB = egorm.Load("dm.test").Build()
}
