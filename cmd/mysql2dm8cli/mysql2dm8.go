package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	gormdm "github.com/gotomicro/gorm-to-dm"
)

func main() {
	var flags struct {
		forceIdentityInsert bool
		onConflictColumns   string
		sql                 string
		sqlFile             string
		outputFile          string
	}
	var RootCmd = &cobra.Command{
		Use:   "mysql2dm8",
		Short: "mysql to dm8",
		Run: func(cmd *cobra.Command, args []string) {
			if flags.sql == "" && flags.sqlFile == "" {
				fmt.Println("sql or sql-file must be set")
				os.Exit(1)
			}
			var onConflictColumns map[string][]string
			if flags.onConflictColumns != "" {
				onConflictColumns = make(map[string][]string)
				for _, tableColumns := range strings.Split(flags.onConflictColumns, ";") {
					tableColumns := strings.Split(tableColumns, ":")
					table := strings.TrimSpace(tableColumns[0])
					columns := strings.Split(tableColumns[1], ",")
					for i := range columns {
						columns[i] = strings.TrimSpace(columns[i])
					}
					onConflictColumns[table] = columns
				}
			}
			if flags.sqlFile != "" {
				sql, err := os.ReadFile(flags.sqlFile)
				if err != nil {
					fmt.Println("read sql file err:", err)
					os.Exit(1)
				}
				flags.sql = string(sql)
			}
			var options []func(option *gormdm.BuildOption)
			if flags.forceIdentityInsert {
				options = append(options, gormdm.WithForceIdentityInsert())
			}
			if len(onConflictColumns) > 0 {
				options = append(options, gormdm.WithOnConflictColumns(onConflictColumns))
			}
			outputSql, err := gormdm.NewBuilder(options...).Parse(flags.sql)
			if err != nil {
				fmt.Println("parse sql err:", err)
				os.Exit(1)
			}
			if flags.outputFile != "" {
				err = os.WriteFile(flags.outputFile, []byte(outputSql), 0666)
				if err != nil {
					fmt.Println("write output file err:", err)
					os.Exit(1)
				}
			} else {
				fmt.Println(outputSql)
			}
		},
	}
	RootCmd.PersistentFlags().BoolVar(&flags.forceIdentityInsert, "force-identity-insert", false, "force identity insert")
	RootCmd.PersistentFlags().StringVar(&flags.onConflictColumns, "on-conflict-columns", "", "on conflict columns, e.g. table1:col1,col2;table2:col1,col2")
	RootCmd.PersistentFlags().StringVar(&flags.sql, "sql", "", "sql")
	RootCmd.PersistentFlags().StringVar(&flags.sqlFile, "sql-file", "", "sql from file")
	RootCmd.PersistentFlags().StringVar(&flags.outputFile, "output-file", "", "output file")
	err := RootCmd.Execute()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
