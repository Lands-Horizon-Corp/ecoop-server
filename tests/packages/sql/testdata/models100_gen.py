# Generates tests/packages/sql/models100_test.go: users, accounts and
# transactions as Bun models across 100 versions.
#
#   python3 tests/packages/sql/testdata/models100_gen.py tests/packages/sql/models100_test.go
#   gofmt -w tests/packages/sql/models100_test.go
import copy, sys, textwrap

class T:
    def __init__(s, table, prefix):
        s.table, s.prefix = table, prefix
        s.cols = []      # (go, type, tag)
        s.idx = []       # (name, sql)
        s.chk = []       # (name, expr)
        s.enums = []     # (type, [values])
        s.ver = None
    def col(s, name):
        return next(i for i, c in enumerate(s.cols) if c[2].split(',')[0] == name)

def camel(n): return ''.join(p.capitalize() if p not in ('id',) else 'ID' for p in n.split('_'))

users, accounts, txs = T('users', 'M100Users'), T('accounts', 'M100Accounts'), T('transactions', 'M100Transactions')
tables = {'users': users, 'accounts': accounts, 'transactions': txs}
live = []           # tables created so far, in order
versions = []       # (v, note, review, [types])

def add(t, name, gotype, tag): t.cols.append((camel(name), gotype, name + ('' if not tag else ',' + tag)))
def drop(t, name): t.cols.pop(t.col(name))
def retag(t, name, tag):
    i = t.col(name); g, ty, _ = t.cols[i]; t.cols[i] = (g, ty, name + ('' if not tag else ',' + tag))
def retype(t, name, gotype, tag):
    i = t.col(name); g, _, _ = t.cols[i]; t.cols[i] = (g, gotype, name + ',' + tag)
def rename(t, old, new):
    i = t.col(old); g, ty, tag = t.cols[i]; t.cols[i] = (camel(new), ty, new + tag[len(old):])
def idx(t, name, sql): t.idx.append((name, sql))
def unidx(t, name): t.idx = [x for x in t.idx if x[0] != name]
def chk(t, name, expr): t.chk.append((name, expr))
def unchk(t, name): t.chk = [x for x in t.chk if x[0] != name]
def enum(t, typ, values):
    t.enums = [e for e in t.enums if e[0] != typ] + [(typ, values)]

emitted = []  # (typename, snapshot of T)
current = {}

def version(note, *changed, review=False):
    v = len(versions) + 1
    for t in changed:
        if t.table not in live: live.append(t.table)
        name = f"{t.prefix}V{v:03d}"
        emitted.append((name, copy.deepcopy(t)))
        current[t.table] = name
    versions.append((v, note, review, [current[n] for n in live]))

PK = ("ID", "int64", "id,pk,autoincrement")
TS = "notnull,default:current_timestamp"

# ---- Phase 1: Foundations (1-20)
users.cols = [PK]; add(users, 'email', 'string', 'type:varchar(255),notnull'); add(users, 'full_name', 'string', 'type:varchar(255),notnull'); add(users, 'created_at', 'time.Time', TS)
version("users: the first table, keyed by bigserial, timestamptz from day one", users)
accounts.cols = [PK]; add(accounts, 'user_id', 'int64', 'notnull'); add(accounts, 'number', 'string', 'type:varchar(32),notnull')
add(accounts, 'balance', 'float64', "type:numeric(14,2),notnull,default:0"); add(accounts, 'created_at', 'time.Time', TS)
version("accounts belong to users (belongs-to foreign key)", accounts)
txs.cols = [PK]; add(txs, 'account_id', 'int64', 'notnull'); add(txs, 'amount', 'float64', 'type:numeric(14,2),notnull'); add(txs, 'kind', 'string', 'type:varchar(16),notnull'); add(txs, 'created_at', 'time.Time', TS)
version("transactions belong to accounts", txs)
idx(users, 'users_email_idx', 'CREATE INDEX users_email_idx ON users (email)'); version("index: users by email", users)
idx(accounts, 'accounts_number_idx', 'CREATE INDEX accounts_number_idx ON accounts (number)'); version("index: accounts by number", accounts)
idx(accounts, 'accounts_user_id_idx', 'CREATE INDEX accounts_user_id_idx ON accounts (user_id)'); version("index: the accounts foreign key", accounts)
idx(txs, 'transactions_account_id_idx', 'CREATE INDEX transactions_account_id_idx ON transactions (account_id)'); version("index: transactions by account", txs)
add(users, 'phone', 'string', 'type:varchar(32),nullzero'); version("users.phone (nullable: catalog-only)", users)
add(accounts, 'currency', 'string', "type:varchar(3),notnull,default:'PHP'"); version("accounts.currency NOT NULL with a constant default", accounts)
add(txs, 'description', 'string', 'type:varchar(255),nullzero'); version("transactions.description (100,000 rows per table are seeded here)", txs)
add(users, 'status', 'string', "type:varchar(16),notnull,default:'active'"); version("users.status as text", users)
idx(txs, 'transactions_created_at_idx', 'CREATE INDEX transactions_created_at_idx ON transactions (created_at)'); version("index: transactions by date", txs)
add(accounts, 'status', 'string', "type:varchar(16),notnull,default:'open'"); version("accounts.status as text", accounts)
add(users, 'last_login_at', 'time.Time', 'nullzero'); version("users.last_login_at", users)
add(txs, 'reference', 'string', 'type:varchar(64),nullzero'); version("transactions.reference, nullable until backfilled", txs)
idx(txs, 'transactions_reference_idx', 'CREATE INDEX transactions_reference_idx ON transactions (reference)'); version("index: transactions by reference", txs)
add(accounts, 'opened_at', 'time.Time', 'nullzero'); version("accounts.opened_at, backfilled from created_at by a data job", accounts)
add(users, 'date_of_birth', 'time.Time', 'type:date,nullzero'); version("users.date_of_birth (date)", users)
add(txs, 'counterparty', 'string', 'type:varchar(255),nullzero'); version("transactions.counterparty", txs)
add(users, 'country', 'string', "type:varchar(2),notnull,default:'PH'"); version("users.country", users)

# ---- Phase 2: Growth & Normalization (21-50)
retag(users, 'email', 'type:varchar(255),notnull,unique'); unidx(users, 'users_email_idx'); version("unique email (constraint) replaces the plain index", users)
retag(accounts, 'number', 'type:varchar(32),notnull,unique'); unidx(accounts, 'accounts_number_idx'); version("unique account number replaces the plain index", accounts)
chk(accounts, 'accounts_balance_check', 'balance >= 0'); version("check: no negative balance", accounts)
chk(txs, 'transactions_amount_check', 'amount <> 0'); version("check: no zero transaction", txs)
enum(txs, 'transaction_kind', ['deposit', 'withdrawal', 'transfer_in', 'transfer_out', 'fee']); retype(txs, 'kind', 'string', 'type:transaction_kind,notnull'); version("enum: transaction kind", txs)
enum(accounts, 'account_status', ['open', 'frozen', 'closed']); retype(accounts, 'status', 'string', "type:account_status,notnull,default:'open'"); version("enum: account status", accounts)
enum(users, 'user_status', ['active', 'suspended', 'closed']); retype(users, 'status', 'string', "type:user_status,notnull,default:'active'"); version("enum: user status", users)
idx(txs, 'transactions_account_created_idx', 'CREATE INDEX transactions_account_created_idx ON transactions (account_id, created_at DESC)'); unidx(txs, 'transactions_account_id_idx'); version("composite index replaces the single-column one", txs)
add(users, 'updated_at', 'time.Time', TS); version("audit: users.updated_at", users)
add(accounts, 'updated_at', 'time.Time', TS); version("audit: accounts.updated_at", accounts)
add(users, 'version', 'int32', 'notnull,default:1'); version("optimistic locking: users.version", users)
add(accounts, 'version', 'int32', 'notnull,default:1'); version("optimistic locking: accounts.version", accounts)
add(users, 'first_name', 'string', 'type:varchar(128),nullzero'); add(users, 'last_name', 'string', 'type:varchar(128),nullzero'); version("split full_name: add the parts (a data job fills them)", users)
retag(users, 'first_name', 'type:varchar(128),notnull'); retag(users, 'last_name', 'type:varchar(128),notnull'); version("split full_name: the parts become required", users)
drop(users, 'full_name'); version("split full_name: the old column goes (reviewed)", users, review=True)
add(txs, 'amount_cents', 'int64', 'nullzero'); version("money in cents: add the column (a data job fills it)", txs)
retag(txs, 'amount_cents', 'notnull'); chk(txs, 'transactions_amount_cents_check', 'amount_cents <> 0'); version("money in cents: required and checked", txs)
drop(txs, 'amount'); unchk(txs, 'transactions_amount_check'); version("money in cents: the numeric column goes (reviewed)", txs, review=True)
add(accounts, 'balance_cents', 'int64', 'notnull,default:0'); version("balance in cents (a data job fills it)", accounts)
chk(accounts, 'accounts_balance_cents_check', 'balance_cents >= 0'); version("check: balance in cents", accounts)
drop(accounts, 'balance'); unchk(accounts, 'accounts_balance_check'); version("balance in cents: the numeric column goes (reviewed)", accounts, review=True)
chk(users, 'users_email_lowercase', 'email = lower(email)'); version("check: emails stored lower-case", users)
retag(accounts, 'user_id', 'notnull,unique:accounts_user_currency_key'); retag(accounts, 'currency', "type:varchar(3),notnull,default:'PHP',unique:accounts_user_currency_key"); version("unique (user, currency): a composite unique key", accounts)
add(txs, 'balance_after_cents', 'int64', 'nullzero'); version("transactions.balance_after_cents", txs)
add(users, 'email_verified_at', 'time.Time', 'nullzero'); version("users.email_verified_at", users)
idx(users, 'users_status_created_idx', 'CREATE INDEX users_status_created_idx ON users (status, created_at)'); version("composite index: users by status and date", users)
enum(accounts, 'account_type', ['checking', 'savings']); add(accounts, 'type', 'string', "type:account_type,notnull,default:'savings'"); version("enum: account type", accounts)
retag(txs, 'reference', 'type:varchar(64),notnull'); version("reference becomes required (a data job backfills it first)", txs)
retag(txs, 'account_id', 'notnull,unique:transactions_account_reference_key'); retag(txs, 'reference', 'type:varchar(64),notnull,unique:transactions_account_reference_key'); unidx(txs, 'transactions_reference_idx'); version("idempotency: unique (account, reference) replaces the reference index", txs)
add(accounts, 'closed_at', 'time.Time', 'nullzero'); chk(accounts, 'accounts_closed_check', "status <> 'closed' OR closed_at IS NOT NULL"); version("cross-column check: a closed account says when", accounts)

# ---- Phase 3: Performance & Advanced Indexing (51-80)
def ix(t, name, sql, note): idx(t, name, sql); version(note, t)
ix(accounts, 'accounts_open_user_idx', "CREATE INDEX accounts_open_user_idx ON accounts (user_id) WHERE status = 'open'", "partial index: open accounts by user")
ix(users, 'users_active_last_login_idx', "CREATE INDEX users_active_last_login_idx ON users (last_login_at DESC) WHERE status = 'active'", "partial index: recently active users")
ix(users, 'users_lower_name_idx', "CREATE INDEX users_lower_name_idx ON users (lower(first_name || ' ' || last_name))", "expression index: case-insensitive names")
add(users, 'metadata', 'map[string]any', "type:jsonb,notnull,default:'{}'"); version("users.metadata (jsonb)", users)
ix(users, 'users_metadata_gin', "CREATE INDEX users_metadata_gin ON users USING gin (metadata jsonb_path_ops)", "GIN (jsonb_path_ops) on user metadata")
add(txs, 'metadata', 'map[string]any', "type:jsonb,notnull,default:'{}'"); version("transactions.metadata (jsonb)", txs)
ix(txs, 'transactions_metadata_gin', "CREATE INDEX transactions_metadata_gin ON transactions USING gin (metadata jsonb_path_ops)", "GIN on transaction metadata")
add(accounts, 'metadata', 'map[string]any', "type:jsonb,notnull,default:'{}'"); version("accounts.metadata (jsonb)", accounts)
ix(accounts, 'accounts_metadata_gin', "CREATE INDEX accounts_metadata_gin ON accounts USING gin (metadata jsonb_path_ops)", "GIN on account metadata")
idx(txs, 'transactions_account_created_cover_idx', "CREATE INDEX transactions_account_created_cover_idx ON transactions (account_id, created_at DESC) INCLUDE (amount_cents)"); unidx(txs, 'transactions_account_created_idx'); version("covering index (INCLUDE) replaces the composite one", txs)
idx(accounts, 'accounts_user_cover_idx', "CREATE INDEX accounts_user_cover_idx ON accounts (user_id) INCLUDE (balance_cents, currency)"); unidx(accounts, 'accounts_user_id_idx'); version("covering index for 'my accounts'", accounts)
idx(txs, 'transactions_created_brin', "CREATE INDEX transactions_created_brin ON transactions USING brin (created_at)"); unidx(txs, 'transactions_created_at_idx'); version("BRIN replaces the btree on created_at", txs)
ix(txs, 'transactions_day_idx', "CREATE INDEX transactions_day_idx ON transactions (date_trunc('day', created_at AT TIME ZONE 'UTC'))", "expression index: UTC day")
ix(txs, 'transactions_debits_idx', "CREATE INDEX transactions_debits_idx ON transactions (account_id) WHERE amount_cents < 0", "partial index: debits only")
ix(accounts, 'accounts_number_upper_idx', "CREATE INDEX accounts_number_upper_idx ON accounts (upper(number))", "expression index: account number, any case")
ix(txs, 'transactions_reference_hash_idx', "CREATE INDEX transactions_reference_hash_idx ON transactions USING hash (reference)", "hash index: equality lookups by reference")
ix(txs, 'transactions_balance_after_idx', "CREATE INDEX transactions_balance_after_idx ON transactions (account_id) INCLUDE (balance_after_cents) WHERE balance_after_cents IS NOT NULL", "partial covering index")
add(users, 'preferences', 'map[string]any', "type:jsonb,notnull,default:'{}'"); version("users.preferences (jsonb)", users)
ix(users, 'users_preferences_gin', "CREATE INDEX users_preferences_gin ON users USING gin (preferences)", "GIN (default opclass) for key lookups")
ix(txs, 'transactions_channel_idx', "CREATE INDEX transactions_channel_idx ON transactions ((metadata ->> 'channel')) WHERE metadata ? 'channel'", "partial expression index on a jsonb key")
ix(accounts, 'accounts_one_open_checking_key', "CREATE UNIQUE INDEX accounts_one_open_checking_key ON accounts (user_id) WHERE type = 'checking' AND status = 'open'", "partial unique index: one open checking account per user")
chk(users, 'users_metadata_object_check', "jsonb_typeof(metadata) = 'object'"); version("check: metadata is a JSON object", users)
add(accounts, 'overdraft_limit_cents', 'int64', 'notnull,default:0'); chk(accounts, 'accounts_overdraft_check', 'overdraft_limit_cents >= 0'); version("accounts.overdraft_limit_cents", accounts)
unchk(accounts, 'accounts_balance_cents_check'); chk(accounts, 'accounts_balance_within_overdraft', 'balance_cents >= -overdraft_limit_cents'); version("the balance rule becomes 'within the overdraft' (check replaced)", accounts)
ix(users, 'users_country_status_idx', "CREATE INDEX users_country_status_idx ON users (country, status)", "composite index: users by country and status")
add(txs, 'external_id', 'string', 'type:uuid,nullzero'); idx(txs, 'transactions_external_id_key', "CREATE UNIQUE INDEX transactions_external_id_key ON transactions (external_id) WHERE external_id IS NOT NULL"); version("transactions.external_id (uuid) with a partial unique index", txs)
ix(accounts, 'accounts_currency_status_idx', "CREATE INDEX accounts_currency_status_idx ON accounts (currency, status)", "composite index: accounts by currency and status")
add(users, 'referral_code', 'string', 'type:varchar(12),nullzero,unique'); version("users.referral_code, unique", users)
ix(txs, 'transactions_counterparty_idx', "CREATE INDEX transactions_counterparty_idx ON transactions (counterparty) WHERE counterparty IS NOT NULL", "partial index: transactions with a counterparty")
add(accounts, 'nickname', 'string', 'type:varchar(64),nullzero'); version("accounts.nickname", accounts)

# ---- Phase 4: Enterprise Scale & Zero-Downtime (81-100)
add(users, 'phone_number', 'string', 'type:varchar(32),nullzero'); version("expand: users.phone_number beside phone", users)
ix(users, 'users_phone_number_idx', "CREATE INDEX users_phone_number_idx ON users (phone_number)", "expand: index the new column (built CONCURRENTLY); a batched data job copies phone over")
drop(users, 'phone'); version("contract: phone goes once readers moved (reviewed)", users, review=True)
add(accounts, 'iban', 'string', 'type:varchar(34),nullzero'); version("expand: accounts.iban, nullable", accounts)
retag(accounts, 'iban', 'type:varchar(34),notnull,unique'); version("iban required and unique (a batched data job fills it first)", accounts)
add(txs, 'counterparty_account_id', 'int64', 'nullzero'); version("transactions.counterparty_account_id, a second foreign key to accounts", txs)
ix(txs, 'transactions_counterparty_account_idx', "CREATE INDEX transactions_counterparty_account_idx ON transactions (counterparty_account_id)", "index the new foreign key (CONCURRENTLY)")
add(users, 'deleted_at', 'time.Time', 'nullzero'); version("soft delete: users.deleted_at", users)
ix(users, 'users_live_email_idx', "CREATE INDEX users_live_email_idx ON users (email) WHERE deleted_at IS NULL", "partial index over live users (CONCURRENTLY)")
enum(txs, 'transaction_kind', ['deposit', 'withdrawal', 'transfer_in', 'transfer_out', 'fee', 'reversal']); version("enum: add a transaction kind (catalog-only)", txs)
enum(accounts, 'account_status', ['open', 'frozen', 'closed', 'dormant']); version("enum: add an account status", accounts)
add(txs, 'settled_at', 'time.Time', 'nullzero'); version("transactions.settled_at", txs)
ix(txs, 'transactions_unsettled_idx', "CREATE INDEX transactions_unsettled_idx ON transactions (created_at) WHERE settled_at IS NULL", "partial index: the settlement queue")
add(users, 'kyc_level', 'int16', 'notnull,default:0'); chk(users, 'users_kyc_level_check', 'kyc_level BETWEEN 0 AND 3'); version("users.kyc_level with a range check", users)
add(accounts, 'interest_rate_bps', 'int32', 'notnull,default:0'); chk(accounts, 'accounts_interest_rate_check', 'interest_rate_bps >= 0'); version("accounts.interest_rate_bps", accounts)
add(txs, 'fee_cents', 'int64', 'notnull,default:0'); chk(txs, 'transactions_fee_check', 'fee_cents >= 0'); version("transactions.fee_cents", txs)
rename(txs, 'description', 'memo'); version("rename description to memo (detected as a rename, instant)", txs)
rename(accounts, 'nickname', 'label'); version("rename nickname to label", accounts)
ix(users, 'users_email_cover_idx', "CREATE INDEX users_email_cover_idx ON users (email) INCLUDE (status)", "covering index for the login path (CONCURRENTLY)")
ix(txs, 'transactions_account_settled_idx', "CREATE INDEX transactions_account_settled_idx ON transactions (account_id, settled_at) WHERE settled_at IS NOT NULL", "partial composite index: settled history (CONCURRENTLY)")
assert len(versions) == 100, len(versions)

# relations: belongs-to fields per table (only on types that have the column)
RELS = {
    'accounts': [("User", "*M100UserRef", "user_id")],
    'transactions': [("Account", "*M100AccountRef", "account_id"), ("CounterpartyAccount", "*M100AccountRef", "counterparty_account_id")],
}

out = []
w = out.append
w("//go:build integration\n")
w("// Code generated by testdata/models100_gen.py; DO NOT EDIT. Change the versions there.\n")
w("package sql_test\n")
w('import (\n\t"context"\n\t"time"\n\n\t"github.com/uptrace/bun"\n)\n')
w("var _ = time.Time{}\n")
w("// M100UserRef and M100AccountRef are what foreign keys point at.\n")
w('type M100UserRef struct {\n\tbun.BaseModel `bun:"table:users"`\n\tID int64 `bun:"id,pk,autoincrement"`\n}\n')
w('type M100AccountRef struct {\n\tbun.BaseModel `bun:"table:accounts"`\n\tID int64 `bun:"id,pk,autoincrement"`\n}\n')
for name, t in emitted:
    w(f"type {name} struct {{")
    w(f'\tbun.BaseModel `bun:"table:{t.table}"`')
    colnames = {c[2].split(',')[0] for c in t.cols}
    for g, ty, tag in t.cols:
        w(f'\t{g} {ty} `bun:"{tag}"`')
    for g, ty, col in RELS.get(t.table, []):
        if col in colnames:
            w(f'\t{g} {ty} `bun:"rel:belongs-to,join:{col}=id"`')
    w("}\n")
    if t.enums:
        w(f"func (*{name}) BeforeCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {{")
        w("\treturn m100Enums(ctx, q,")
        for typ, vals in t.enums:
            w(f'\t\tm100Enum{{"{typ}", []string{{{", ".join(chr(34)+v+chr(34) for v in vals)}}}}},')
        w("\t)\n}\n")
    if t.idx or t.chk:
        w(f"func (*{name}) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {{")
        w("\treturn m100DDL(ctx, q,")
        for n, sql in t.idx:
            w(f'\t\t`{sql}`,')
        for n, expr in t.chk:
            w(f'\t\t`ALTER TABLE {t.table} ADD CONSTRAINT {n} CHECK ({expr})`,')
        w("\t)\n}\n")
w("// m100Versions lists each version's models, 1 to 100.")
w("var m100Versions = []m100Version{")
for v, note, review, types in versions:
    w(f'\t{{v: {v}, note: "{note}", review: {"true" if review else "false"}, models: []any{{{", ".join("(*"+t+")(nil)" for t in types)}}}}},')
w("}")
open(sys.argv[1], "w").write("\n".join(out) + "\n")
print("types", len(emitted), "versions", len(versions))
