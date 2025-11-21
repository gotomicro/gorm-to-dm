package gormdm

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ego-component/egorm/manager"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var (
	errInvalidDSNUnescaped                   = errors.New("invalid DSN: did you forget to escape a param value")
	errInvalidDSNAddr                        = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	_                      manager.DSNParser = (*DmDSNParser)(nil)
	_                      schema.Namer      = (*NamingStrategy)(nil)
)

type DmDSNParser struct {
}

func init() {
	manager.Register(&DmDSNParser{})
}

func (d *DmDSNParser) Scheme() string {
	return "dm"
}

func (d *DmDSNParser) NamingStrategy() schema.Namer {
	return &NamingStrategy{}
}

func (d *DmDSNParser) GetDialector(dsn string) gorm.Dialector {
	cfg, err := d.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	dsn = dmDsn(cfg)
	return Open(dsn)
}

func (d *DmDSNParser) ParseDMDSN(dsn string) (cfg *manager.DSN, err error) {
	// New config with some default values
	cfg = new(manager.DSN)
	u, err := url.Parse(fmt.Sprintf("dm://%s", dsn))
	if err != nil {
		return nil, err
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	cfg.Addr = u.Host
	q := u.Query()
	cfg.DBName = q.Get("schema")
	if cfg.DBName == "" && u.Path != "" {
		cfg.DBName = strings.Replace(u.Path, "/", "", 1)
	}
	cfg.Net = "tcp"
	cfg.Params = make(map[string]string, len(q))
	for k, v := range q {
		cfg.Params[k] = v[0]
	}
	return cfg, nil
}

func (m *DmDSNParser) ParseDSN(dsn string) (cfg *manager.DSN, err error) {
	// 兼容老版本的达梦标准配置
	cfg, err = m.ParseDMDSN(dsn)
	if err == nil {
		return cfg, nil
	}

	// New config with some default values
	cfg = new(manager.DSN)

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						parseUsernamePassword(cfg, dsn[:j])
						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				if err = parseAddrNet(cfg, dsn[j:i]); err != nil {
					return
				}
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}
	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}
	return
}

// username[:password]
func parseUsernamePassword(cfg *manager.DSN, userPassStr string) {
	for i := 0; i < len(userPassStr); i++ {
		if userPassStr[i] == ':' {
			cfg.Password = userPassStr[i+1:]
			cfg.User = userPassStr[:i]
			break
		}
	}
}

// [protocol[(address)]]
func parseAddrNet(cfg *manager.DSN, addrNetStr string) error {
	for i := 0; i < len(addrNetStr); i++ {
		if addrNetStr[i] == '(' {
			// dsn[i-1] must be == ')' if an address is specified
			if addrNetStr[len(addrNetStr)-1] != ')' {
				if strings.ContainsRune(addrNetStr[i+1:], ')') {
					return errInvalidDSNUnescaped
				}
				return errInvalidDSNAddr
			}
			cfg.Addr = addrNetStr[i+1 : len(addrNetStr)-1]
			cfg.Net = addrNetStr[1:i]
			break
		}
	}
	return nil
}

var dmSupportParams = map[string]string{
	"timeout": "connectTimeout",
}

var dmTimeParams = map[string]bool{
	"connectTimeout": true,
}

// param1=value1&...&paramN=valueN
func parseDSNParams(cfg *manager.DSN, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		// lazy init
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		name := param[0]
		value := param[1]
		if _, ok := dmSupportParams[name]; !ok {
			continue
		}
		name = dmSupportParams[name]
		if _, ok := dmTimeParams[name]; ok {
			intVal, err := time.ParseDuration(value)
			if err != nil {
				continue
			}
			value = strconv.FormatInt(intVal.Milliseconds(), 10)
		}
		if cfg.Params[name], err = url.QueryUnescape(value); err != nil {
			return
		}
	}
	return
}

func dmDsn(cfg *manager.DSN) string {
	// 用 url.Values 组装 query string
	values := url.Values{}
	for k, v := range cfg.Params {
		if k == "schema" {
			continue
		}
		values.Set(k, v)
	}
	params := values.Encode()
	dbName := cfg.DBName
	// 如果 dbName 没有以 " 开头或结尾，则添加双引号
	if !strings.HasPrefix(dbName, `"`) || !strings.HasSuffix(dbName, `"`) {
		dbName = `"` + dbName + `"`
	}

	dsn := fmt.Sprintf("%s:%s@%s?schema=%s", cfg.User, cfg.Password, cfg.Addr, dbName)
	if params != "" {
		dsn = fmt.Sprintf("%s&%s", dsn, params)
	}
	return dsn
}
