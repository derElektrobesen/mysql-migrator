#!/bin/bash

set -eou pipefail
#set -o xtrace

trap "exit 1" TERM
PID=$$

work_dir="$PWD"
bin_dir="$PWD"
dry_run=0
cleanup=0
trace=0
do_migrate_schema=1
tables_to_migrate=()
skip_tables=()

tmp_file=$(mktemp) # could be used to store error message
trap "drop_tmp_file $tmp_file" EXIT

declare -A mysql_conf
declare -A postgres_conf

declare -A allowed_flags=(
	["--help,method"]=usage
	["--help,help"]="Print this help message and exit"

	["--trace,method"]=enable_trace
	["--trace,help"]="Enable tracing"

	["--cleanup,method"]=enable_cleanup
	["--cleanup,help"]="Cleanup dst database before migration"

	["--dry-run,method"]=enable_dry_run
	["--dry-run,help"]="Run script in dry-run mode"

	["--no-schema-migration,method"]=disable_schema_migration
	["--no-schema-migration,help"]="Skip schema migration using pgloader. Useful for migration restarting"

	["--mysql,method"]=parse_mysql_dsn
	["--mysql,has_arg"]=1
	["--mysql,required"]=1
	["--mysql,help"]="MySQL datasource name in format login:pass@host:port/database"

	["--postgres,method"]=parse_postgres_dsn
	["--postgres,has_arg"]=1
	["--postgres,required"]=1
	["--postgres,help"]="PostgreSQL datasource name in format login:pass@host:port/database"

	["--postgres-schema-name,method"]=set_schema_name
	["--postgres-schema-name,has_arg"]=1
	["--postgres-schema-name,help"]="PostgreSQL schema name for migrated tables, default is the mysql database name"

	["--work-dir,method"]=set_work_dir
	["--work-dir,has_arg"]=1
	["--work-dir,help"]="Script work directory (default: $work_dir)"

	["--bin-dir,method"]=set_bin_dir
	["--bin-dir,has_arg"]=1
	["--bin-dir,help"]="Directory with binaries (default: $bin_dir)"

	["--tables,method"]=set_tables_to_migrate
	["--tables,has_arg"]=1
	["--tables,help"]="Comma-separated list of tables to migrate, (default: all)"

	["--skip-tables,method"]=set_skip_tables
	["--skip-tables,has_arg"]=1
	["--skip-tables,help"]="Comma-separated list of tables to skip, (default: none)"
)

declare -A binaries
required_global_tools=(
	mysql
	psql
)
required_local_tools=(
	pgloader
	conduit
)

###########################################
# Preparing
###########################################

function run() {
	parse_args "$@"
	validate_env

	if [ "${postgres_conf[schema_name]+xxx}" == "" ]; then
		postgres_conf[schema_name]=${mysql_conf[db_name]}
	fi

	run_migration
}

function parse_args() {
	argc=$#
	argv=("$@")

	local required_flags=$(echo "${!allowed_flags[@]}" | tr ' ' '\n' | grep required | awk -F, '{print $1}' | sort -u)
	local required_flags_passed=()

	for (( i=0; i<argc; i++ )); do
		varname="${argv[i]}"

		if [ "${allowed_flags["$varname,method"]+xxx}" != "" ]; then
			varval=""

			local has_arg="${allowed_flags["$varname,has_arg"]+xxx}"
			if [ "$has_arg" != "" ] && [ "$has_arg" != "0xxx" ]; then
				i=$((i+1))
				varval="${argv[i]}"
			fi

			eval "${allowed_flags["$varname,method"]}" "$varval"
		else
			echo "Unknown flag passed: $varname"
			echo

			usage
			exit 1
		fi

		if [ "${allowed_flags["$varname,required"]+xxx}" != "" ]; then
			required_flags_passed+=("$varname")
		fi
	done

	local not_found=$(echo "${required_flags[@]} ${required_flags_passed[@]}" | tr ' ' '\n' | sort | uniq -c | grep -vP '^\s*2 ' | awk '{print $2}')

	for x in "${not_found[@]}"; do
		if [ "$x" != "" ]; then
			echo "Parametr $x is required"
			echo
			usage
		fi
	done
}

function enable_dry_run() {
	dry_run=1
}

function enable_cleanup() {
	cleanup=1
}

function disable_schema_migration() {
	do_migrate_schema=0
}

function set_schema_name() {
	postgres_conf[schema_name]=$1
}

function set_tables_to_migrate() {
	tables_to_migrate=($(echo "$1" | tr ',' '\n'))
}

function set_skip_tables() {
	skip_tables=($(echo "$1" | tr ',' '\n'))
}

function enable_trace() {
	trace=1
}

function check_dir() {
	local d="$1"

	if [ ! -d "$d" ]; then
		error "Directory $d doesn't exists"
	fi
}

function set_work_dir() {
	work_dir=$1
	check_dir $work_dir
}

function set_bin_dir() {
	bin_dir=$1
	check_dir $bin_dir
}

function usage() {
	echo "Mysql to another datasource migration tool"
	echo "Usage: $0 <flags>"
	echo
	echo "Available flags:"

	local available_flags=$(echo "${!allowed_flags[@]}" | tr ' ' '\n' | awk -F, '{print $1}' | sort -u)

	local max_flag_len=5
	for f in $available_flags; do
		cur_flag_len=$(echo -n "$f" | wc -c)
		if [[ "$max_flag_len" -lt "$cur_flag_len" ]]; then
			max_flag_len="$cur_flag_len"
		fi
	done

	max_flag_len=$((max_flag_len+5))
	local fmt_str="\t%${max_flag_len}s : %s%s\n"
	for flag in $available_flags; do
		local suffix=""
		if [ "${allowed_flags["$flag,required"]+xxx}" != "" ]; then
			suffix=" (required)"
		fi

		printf "$fmt_str" "$flag" "${allowed_flags["$flag,help"]}" "$suffix"
	done

	exit 0
}

function parse_mysql_dsn() {
	local dsn="$1"

	parse_dsn "MySQL" mysql_conf "$dsn"
}

function parse_postgres_dsn() {
	local dsn="$1"

	parse_dsn "PostgreSQL" postgres_conf "$dsn"
}

function parse_dsn() {
	local src="$1" # mysql // postgresql // tarantool
	local dst="$2" # varname to store config
	local str="$3" # DSN string to parse

	local -n dst_conf="$dst"

	local dsn=$(echo "$str" | awk -F/ '{print $1}')
	local db_name=$(echo "$str" | awk -F/ '{print $2}')

	# Format: login:pass@host:port
	local login_pass=$(echo "$dsn" | awk -F@ '{print $1}')
	local host_port=$(echo "$dsn" | awk -F@ '{print $2}')

	local login=$(echo "$login_pass" | awk -F: '{print $1}')
	local pass=$(echo "$login_pass" | awk -F: '{print $2}')

	local host=$(echo "$host_port" | awk -F: '{print $1}')
	local port=$(echo "$host_port" | awk -F: '{print $2}')

	if [ "$login" == "" ] ||
		[ "$pass" == "" ] ||
		[ "$host" == "" ] ||
		[ "$port" == "" ] ||
		[ "$db_name" == "" ]
	then
		error "Bad $src connection string: $dsn. Format: login:pass@host:port/database"
	fi

	re='^[0-9]+$'
	if ! [[ $port =~ $re ]]; then
		error "Bad $src connection string: $dsn. Format: login:pass@host:port/database"
	fi

	dst_conf[login]="$login"
	dst_conf[pass]="$pass"
	dst_conf[host]="$host"
	dst_conf[port]="$port"
	dst_conf[db_name]="$db_name"
}

function validate_env() {
	local bad_tool=0

	local required_tools=( ${required_global_tools[@]} )
	for x in "${required_local_tools[@]}"; do
		required_tools+=( "$bin_dir/$x" )
	done

	for x in "${required_tools[@]}"; do
		local p=$(command -v "$x")
		if [ "$p" == "" ]; then
			message "command not found: $x"
			bad_tool=1
		else
			trace "$(basename $p) is found at $(realpath $p)"
			binaries[$(basename $p)]=$(realpath $p)
		fi
	done

	if [ "$bad_tool" == "1" ]; then
		error "install all tools first"
	fi
}

function drop_tmp_file() {
	if [ "$tmp_file" != "" ]; then
		rm "$tmp_file"
	fi
}

###########################################
# Migration
###########################################

function make_pgloader_tables_to_migrate_cmd() {
	local tables_to_import_q=""
	tables_to_import_q="INCLUDING ONLY TABLE NAMES MATCHING"

	local n=0
	local tables=( $(list_source_tables) )
	for t in "${tables[@]}"; do
		n=$((n+1))

		local suffix=""
		if [ $n -lt ${#tables[@]} ]; then
			suffix=","
		fi

		tables_to_import_q="$tables_to_import_q '$t'$suffix"
	done

	printf "$tables_to_import_q\n"
}

function make_pgloader_rename_queries() {
	local table_name="$1"
	local -n queries="$2"

	local table_name_lc=$(echo "$table_name" | perl -ne 'print lc')
	if [ "$table_name_lc" != "$table_name" ]; then
		queries+=("ALTER TABLE $(q ${postgres_conf[schema_name]}).$(q $table_name_lc) RENAME TO $(q $table_name)")
	fi

	for col_name in $(list_columns_in_source_table $table_name); do
		local col_name_lc=$(echo "$col_name" | perl -ne 'print lc')

		if [ "$col_name" != "$col_name_lc" ]; then
			queries+=("ALTER TABLE $(q ${postgres_conf[schema_name]}).$(q $table_name) RENAME COLUMN $(q $col_name_lc) TO $(q $col_name)")
		fi
	done
}

function make_pgloader_after_queries() {
	local prev_schema_name=$(echo "${mysql_conf[db_name]}" | perl -ne 'print lc') # pgloader migrates without case respect

	local after_queries=()
	if [ "$prev_schema_name" != "${postgres_conf[schema_name]}" ]; then
		after_queries+=("ALTER SCHEMA $(q $prev_schema_name) RENAME TO $(q ${postgres_conf[schema_name]})")
		after_queries+=("ALTER DATABASE $(q ${postgres_conf[db_name]}) SET search_path = $(q ${postgres_conf[schema_name]}), public")
	fi

	for t in $(list_source_tables); do
		make_pgloader_rename_queries "$t" after_queries
	done

	local after_load_do=""
	if [ ${#after_queries[@]} -ne 0 ]; then
		after_load_do="AFTER LOAD DO"

		local n=0
		for q in "${after_queries[@]}"; do
			n=$((n+1))

			local suffix=""
			if [ $n -lt ${#after_queries[@]} ]; then
				suffix=","
			fi

			after_load_do=$(printf "$after_load_do\n        \$\$%s\$\$%s" "$q" "$suffix")
		done

	fi

	echo "$after_load_do"
}

function make_pgloader_script() {
	local filename="$1"

	local after_load_do=$(make_pgloader_after_queries)
	local tables_to_import_q=$(make_pgloader_tables_to_migrate_cmd)

	cat > $filename <<- EOM
LOAD DATABASE
    FROM      mysql://${mysql_conf[login]}:${mysql_conf[pass]}@${mysql_conf[host]}:${mysql_conf[port]}/${mysql_conf[db_name]}
    INTO postgresql://${postgres_conf[login]}:${postgres_conf[pass]}@${postgres_conf[host]}:${postgres_conf[port]}/${postgres_conf[db_name]}
    WITH SCHEMA ONLY
    $tables_to_import_q
    $after_load_do
;
EOM
}

function call_psql_real() {
	local cmd="$1"
	local db_name="$2"

	echo "$cmd" | \
		PGPASSWORD="${postgres_conf[pass]}" ${binaries[psql]} \
			--host ${postgres_conf[host]} \
			--port ${postgres_conf[port]} \
			--username ${postgres_conf[login]} \
			$db_name 2>"$tmp_file" | \
		tail -n +3 | \
		head -n -2
}

function call_mysql_real() {
	local cmd="$1"
	local db_name="$2"

	echo "$cmd" | \
		MYSQL_PWD="${mysql_conf[pass]}" ${binaries[mysql]} \
			--host=${mysql_conf[host]} \
			--port=${mysql_conf[port]} \
			--user=${mysql_conf[login]} \
			$db_name 2>"$tmp_file" | \
		tail -n +2
}

function call_db_expect_error() {
	# Usage: read res err < <(call_db_expect_error ...)
	# If expect_error == 1, res will contain all lines with `|` delimiter
	# err contains error message

	local cmd="$1"
	local expect_error="$2"
	local db_name="$3"
	local callee="$4"

	trace "DB $db_name; QUERY: $cmd"

	local res=$($callee "$cmd" "$db_name")

	local err_msg=""
	if [ -s "$tmp_file" ]; then
		err_msg=$(cat "$tmp_file")
	fi

	if [ "$expect_error" == "1" ]; then
		res=$(echo "$res" |\
			perl -pe 's/^\s*|\s*$//' | \
			tr '\n' '|' | \
			perl -pe 's/\|$//' \
		)
		echo "$res" "$err_msg"
	else
		if [ "$err_msg" != "" ]; then
			error "unable to execute query: $err_msg"
		fi
		echo "$res" # multiline result
	fi
}

function call_psql_expect_error() {
	local cmd="$1"
	local expect_error="$2"

	local db_name="${postgres_conf[db_name]}"
	if [[ "${3+xxx}" == "xxx" ]] && [ "$3" != "" ]; then
		db_name="$3"
	fi

	call_db_expect_error "$cmd" "$expect_error" "$db_name" call_psql_real
}

function call_psql() {
	# multiline result will be returned
	local db_name=""
	if [[ "${2+xxx}" == "xxx" ]]; then
		db_name="$2"
	fi

	call_psql_expect_error "$1" 0 "$db_name"
}

function call_mysql_expect_error() {
	local cmd="$1"
	local expect_error="$2"

	local db_name="${mysql_conf[db_name]}"
	if [[ "${3+xxx}" == "xxx" ]] && [ "$3" != "" ]; then
		db_name="$3"
	fi

	call_db_expect_error "$cmd" "$expect_error" "$db_name" call_mysql_real
}

function call_mysql() {
	# multiline result will be returned
	local db_name=""
	if [[ "${2+xxx}" == "xxx" ]]; then
		db_name="$2"
	fi

	call_mysql_expect_error "$1" 0 "$db_name"
}

function modify() {
	local cmd="$1"

	if [ $dry_run -eq 1 ]; then
		info "dry-ryn: skip"
	else
		eval "$cmd"
	fi
}

function create_dst_database_impl() {
	call_psql "CREATE DATABASE $(q ${postgres_conf[db_name]})" "postgres" > /dev/null
}

function check_dst_db_exists() {
	local err_msg
	local res
	read res err_msg < <(call_psql_expect_error 'SELECT 1' 1)

	if [ "$res" == "1" ]; then
		# database already exists
		echo 1
		return
	fi

	local x=$(echo "$err_msg" | grep "database "\""${postgres_conf[db_name]}"\"" does not exist")
	if [ "$x" == "" ]; then
		error "unknown error expected during postgres database verification: $err_msg"
	fi

	echo 0
}

function create_dst_database() {
	if [ "$(check_dst_db_exists)" == "1" ]; then
		# database already exists
		info "database ${postgres_conf[db_name]} found in postgres, continue"
		return
	fi

	info "database ${postgres_conf[db_name]} not found in postgres. $(magenta 'Create one')"
	modify create_dst_database_impl
}

function prepare_dst_database() {
	create_dst_database
}

function migrate_schema() {
	local w_dir=$(realpath "$work_dir/pgloader")

	if [ $cleanup -eq 1 ]; then
		warning "delete directory $(magenta $w_dir)"
		modify "rm -rf $w_dir"
	fi

	mkdir -p $w_dir

	local log_file="$w_dir/pgloader.log"
	rm -f $log_file

	local summary_file="$w_dir/pgloader_summary.log"

	local load_script="$w_dir/migrate.load"
	make_pgloader_script "$load_script"

	local extra_args="--summary $summary_file --root-dir $w_dir --logfile $log_file --on-error-stop"
	if [ $dry_run -eq 1 ]; then
		extra_args="$extra_args --dry-run"
	fi

	if [ $trace -eq 1 ]; then
		extra_args="$extra_args --verbose --debug"
	fi

	info "migrating schema using pgloader..."
	${binaries[pgloader]} $extra_args "$load_script"
	info "log file is available here: $(magenta $log_file)"

	if cat $log_file | grep -q 'FATAL'; then
		error "schema migration failed"
	fi

	info "summary file is available here: $(magenta $summary_file)"
}

function cleanup_postgres() {
	if [ "$(check_dst_db_exists)" == "0" ]; then
		info "database ${postgres_conf[db_name]} not found in postgres, nothing to cleanup. Continue"
		return
	fi

	warning "are you sure you want to cleanup postgres? ${postgres_conf[db_name]} database will be dropped"

	local answer
	read -p "Type 'yes' to continue: " answer
	if [ "$answer" != "yes" ]; then
		error "cancelled"
	fi

	local res
	local err_msg
	read res err_msg < <(call_psql_expect_error "DROP DATABASE IF EXISTS $(q ${postgres_conf[db_name]})" 1 postgres)
	if [ "$err_msg" != "" ] && [ "$err_msg" != "database "\""${postgres_conf[db_name]}"\"" does not exist, skipping" ]; then
		error "unable to drop database ${postgres_conf[db_name]}: $err_msg"
	fi

	info "database ${postgres_conf[db_name]} $(red dropped)"
}

function run_cleanup() {
	info "start cleanup..."
	modify stop_conduit
	modify cleanup_postgres
}

function toggle_indexes() {
	local table_name="$1"
	local action="$2" # ENABLE / DISABLE

	call_psql "ALTER TABLE $(q ${postgres_conf[schema_name]}).$(q $table_name) $action TRIGGER ALL" > /dev/null
}

function disable_indexes_impl() {
	toggle_indexes "$1" "DISABLE"
}

function list_tables() {
	# Function returns a list of all tables in Postgres DB.
	# XXX: List could contain not migrated tables
	# TODO: check database is empty before migration
	call_psql "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = '${postgres_conf[schema_name]}'"
}

function list_source_tables_wo_skip() {
	# If there is a --tables option, function checks all tables in the list are present and
	# returns this list in case-sensitive mode
	#
	# If --tables option is not present, returns all tables in database

	local tables=$(call_mysql "$(\
		echo "SELECT table_name FROM " \
			"INFORMATION_SCHEMA.TABLES " \
			"WHERE TABLE_TYPE = 'BASE TABLE' " \
			"AND TABLE_SCHEMA = '${mysql_conf[db_name]}'" \
		)")

	if [ ${#tables_to_migrate[@]} -eq 0 ]; then
		echo "$tables"
		return
	fi

	for t in "${tables_to_migrate[@]}"; do
		local res=$(echo "$tables" | grep -iw "$t")
		if [ "$res" == "" ]; then
			error "table '$t' not found in mysql database"
		fi

		echo "$res"
	done
}

function list_source_tables() {
	local res="$(list_source_tables_wo_skip)"
	for t in "${skip_tables[@]}"; do
		res="$(echo "$res" | grep -iv "$t")"
	done

	echo "$res"
}

function list_columns_in_source_table() {
	local table_name="$1"
	call_mysql "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='${mysql_conf[db_name]}' AND TABLE_NAME='$table_name'"
}

function disable_indexes() {
	local res=$(list_tables)

	if [ "$res" == "" ]; then
		error "unable to select table names: no tables found"
	fi

	for t in $(echo "$res"); do
		info "disable indexes on table $t"
		modify "disable_indexes_impl '$t'"
	done
}

function setup_base_conduit_config() {
	local -n conf="$1"

	local log_level="info"
	if [ $trace -eq 1 ]; then
		log_level="trace"
	fi

	cat > ${conf[cfg_file]} <<- EOM
api:
    enabled: false
connectors:
    path: ${conf[connectors_dir]}
db:
    badger:
        path: ${conf[badger_db_dir]}
    type: badger
log:
    format: json
    level: $log_level
pipelines:
    error-recovery:
        backoff-factor: 2
        max-delay: 10m0s
        max-retries: -1
        max-retries-window: 5m0s
        min-delay: 1s
    exit-on-degraded: true
    path: ${conf[pipelines_dir]}
preview:
    pipeline-arch-v2: false
processors:
    path: ${conf[processors_dir]}
schema-registry:
    type: builtin
EOM
}

function mysql_server_version() {
	# 55 // 57 // 80 will be returned
	call_mysql "SELECT version()" | awk -F. '{print $1$2}'
}

function setup_conduit_pipeline() {
	local cfg_file="$1"
	local pipeline_name="$(basename $cfg_file '.yaml')-pipeline"

	local tables_list=$(list_tables | tr '\n' ',' | tr -d ' ' | perl -pe 's/,$//')

	local mysql55Compatibility="false"
	if [ $(mysql_server_version) -lt 57 ]; then
		mysql55Compatibility="true"
	fi

	cat > $cfg_file <<- EOM
version: "2.2"
pipelines:
  - id: ${pipeline_name}
    status: running
    name: "$pipeline_name"
    connectors:
      - id: mysql-datasource
        type: source
        plugin: "builtin:mysql" # https://github.com/conduitio-labs/conduit-connector-mysql
        settings:
          dsn: "${mysql_conf[login]}:${mysql_conf[pass]}@tcp(${mysql_conf[host]}:${mysql_conf[port]})/${mysql_conf[db_name]}"
          tables: "$tables_list"
          fetchSize: 100000
          mysql55Compatibility: $mysql55Compatibility

          sdk.batch.size: 100000
          sdk.batch.delay: 100ms # TODO: https://github.com/ConduitIO/conduit-commons/issues/169
      - id: postgresql-destination
        type: destination
        plugin: "builtin:psql" # https://github.com/ConduitIO/conduit-connector-postgres
        settings:
          url: "postgresql://${postgres_conf[login]}:${postgres_conf[pass]}@${postgres_conf[host]}:${postgres_conf[port]}/${postgres_conf[db_name]}"

          sdk.batch.size: 100000
          sdk.batch.delay: 100ms
EOM
}

function setup_conduit_run_script() {
	local path="$1"
	local -n conf="$2"

	cat > $path <<- EOM
#!/bin/bash

cd $(dirname ${conf[processors_dir]})
echo "\$\$" > ${conf[pid_file]}
${binaries[conduit]}
EOM

	chmod +x $path
}

function run_conduit_real() {
	local run_script="$1"
	local log_file="$2"
	local pid_file="$3"

	# Detach new process from console
	setsid $run_script > $log_file 2>&1 < /dev/null &

	sleep 3 # wait for conduit running

	local pids="$(conduit_pids $pid_file)"
	info "conduit is running. PIDs are: $(magenta $pids)"
	info "use $(magenta INT) signal to stop conduit gracefully"
	info "conduit log is available in $(magenta $log_file)"
}

function conduit_pids() {
	local pid_file="$1"
	if [ -f $pid_file ]; then
		ps -s $(cat $pid_file) -o pid= | tr '\n' ' ' | perl -pe 's/\s+/ /'
	fi
}

function stop_already_running_conduit() {
	local pid_file="$1"
	local signal="INT"

	if [ "${2+xxx}" == "xxx" ] && [ "$2" != "" ]; then
		signal="$2"
	fi

	local pids=$(conduit_pids $pid_file)
	
	if [ "$pids" != "" ]; then
		info "conduit is already running. Stop it"
		modify "kill -$signal $pids && sleep 5"
	fi
}

function stop_conduit() {
	declare -A conduit_conf
	configure_conduit conduit_conf

	stop_already_running_conduit ${conduit_conf[pid_file]} KILL
	rm -f ${conduit_conf[pid_file]}
}

function configure_conduit() {
	local -n ref="$1"

	local w_dir=$(realpath "$work_dir/conduit")

	ref[work_dir]="$w_dir"
	ref[processors_dir]="$w_dir/processors"
	ref[connectors_dir]="$w_dir/connectors"
	ref[pipelines_dir]="$w_dir/pipelines"
	ref[badger_db_dir]="$w_dir/badger.db"
	ref[cfg_file]="$w_dir/conduit.yaml"
	ref[log_file]="$w_dir/conduit.log"
	ref[pid_file]="$w_dir/conduit.pid"
}

function run_conduit() {
	declare -A conduit_conf
	configure_conduit conduit_conf

	local w_dir="${conduit_conf[work_dir]}"
	
	if [ $cleanup -eq 1 ]; then
		warning "delete directory $(magenta $w_dir)"
		modify "rm -rf $w_dir"
	fi

	mkdir -p $w_dir
	mkdir -p ${conduit_conf[processors_dir]}
	mkdir -p ${conduit_conf[connectors_dir]}
	mkdir -p ${conduit_conf[pipelines_dir]}
	mkdir -p ${conduit_conf[badger_db_dir]}

	setup_base_conduit_config conduit_conf
	setup_conduit_pipeline "${conduit_conf[pipelines_dir]}/mysql-to-postgres-${mysql_conf[db_name]}.yaml"

	stop_already_running_conduit ${conduit_conf[pid_file]}

	local run_script="$w_dir/run_conduit.sh"
	setup_conduit_run_script $run_script conduit_conf

	if [ -f ${conduit_conf[log_file]} ]; then
		local f=${conduit_conf[log_file]}
		local n=0
		while [ -f "$f.$n" ]; do
			n=$((n+1))
		done

		info "backup previous log file as $(magenta "$f.$n")"
		cp $f $f.$n
	fi

	info "starting conduit..."
	modify "run_conduit_real $run_script ${conduit_conf[log_file]} ${conduit_conf[pid_file]}"
}

function run_migration() {
	if [ $cleanup -eq 1 ]; then
		run_cleanup
	fi

	prepare_dst_database

	if [ $do_migrate_schema -eq 1 ]; then
		migrate_schema
	fi

	disable_indexes
	run_conduit

	# TODO
	#  * check mysql instance is in read-only mode
	#  * check replication is disabled
	#  * check there is no load on replica
}

###########################################
# Helpers
###########################################

function q() {
	echo ""\""$1"\"""
}

#{{{ Colors
function red() {
	echo -e "\e[1;31m$@\e[0m"
}

function green() {
	echo -e "\e[1;32m$@\e[0m"
}

function yellow() {
	echo -e "\e[1;33m$@\e[0m"
}

function magenta() {
	echo -e "\e[1;95m$@\e[0m"
}
#}}}

#{{{ Logging
function trace() {
	if [ $trace -eq 1 ]; then
		message "$(yellow 'trace: ') $@"
	fi
}

function info() {
	message "$(yellow 'info: ') $@"
}

function warning() {
	message "$(red 'warning: ') $@"
}

function error() {
	message "$(red 'error: ') $@"

	# we can't just call `exit` because we need to kill the script at all, but exit not works in commands like this:
	# $ x=$(exit)
	# $ echo $?
	# > 0
	kill -s TERM -$PID
}

function message() {
	>&2 echo -e "$@"
}

#}}}

run "$@"
