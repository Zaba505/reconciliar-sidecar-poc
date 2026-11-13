package gen

import "strings"

const (
	alnum  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	digits = "0123456789"
)

// list returns a list length distribution.
func list(median, sigma float64, lo, hi int) *Dist {
	return &Dist{Median: median, Sigma: sigma, Min: lo, Max: hi}
}

// Payment is an internal ledger's payment, the left side of a run: a
// nested counterparty, fees in the payment's currency, tags, and a rare
// metadata list with a long tail.
var Payment = &Schema{Name: "Payment", Fields: []Field{
	{Name: "id", Type: TypeIdentity},
	{Name: "payment_ref", Type: TypeString, Source: SourceKey},
	{Name: "account_id", Type: TypeString, Source: SourceDict, Dict: DictIBAN, Card: 20000, Skew: 1.1},
	{Name: "counterparty", Type: TypeRecord, TypeName: "Party", Fields: []Field{
		{Name: "name", Type: TypeString, Source: SourceDict, Dict: DictName, Card: 5000, Skew: 1.0},
		{Name: "account", Type: TypeString, Source: SourceDict, Dict: DictIBAN, Card: 50000, Skew: 0.8},
		{Name: "bic", Type: TypeString, Source: SourceDict, Dict: DictBIC, Card: 500, Skew: 1.1, Optional: 0.3},
		{Name: "country", Type: TypeEnum, TypeName: "Country", Symbols: []string{"GB", "DE", "FR", "NL", "ES", "IT", "BE", "IE", "US", "CH"}, Weights: []float64{20, 18, 12, 8, 8, 8, 4, 4, 14, 4}},
	}},
	{Name: "type", Type: TypeEnum, TypeName: "PaymentType", Symbols: []string{"PAYMENT", "TRANSFER", "CARD", "FEE", "FX", "REFUND", "INTEREST"}, Weights: []float64{40, 20, 10, 10, 7, 8, 5}},
	{Name: "direction", Type: TypeEnum, TypeName: "Direction", Symbols: []string{"DEBIT", "CREDIT"}, Weights: []float64{55, 45}},
	{Name: "amount", Type: TypeMoney, Median: 12000, Sigma: 1.6},
	{Name: "fx_rate", Type: TypeDouble, Median: 1.1, Sigma: 1.2, Optional: 0.85},
	{Name: "booked_at", Type: TypeTimestamp, Window: 30 * day},
	{Name: "value_date", Type: TypeDate, Lag: 2},
	{Name: "status", Type: TypeEnum, TypeName: "PaymentStatus", Symbols: []string{"SETTLED", "PENDING", "REVERSED", "FAILED"}, Weights: []float64{85, 10, 3, 2}},
	{Name: "channel", Type: TypeEnum, TypeName: "Channel", Symbols: []string{"API", "BATCH", "SWIFT", "SEPA", "ACH", "CARD_NETWORK"}, Weights: []float64{30, 25, 10, 15, 15, 5}},
	{Name: "end_to_end_id", Type: TypeString, Source: SourceToken, Alphabet: alnum, Len: Dist{Median: 24, Sigma: 0.2, Min: 12, Max: 35}, Optional: 0.4},
	{Name: "batch_id", Type: TypeInt64, Min: 1, Max: 50000},
	{Name: "memo", Type: TypeString, Source: SourceText, Len: Dist{Median: 6, Sigma: 0.7, Min: 1, Max: 140}, Optional: 0.25},
	{Name: "created_by", Type: TypeString, Source: SourceDict, Dict: DictUser, Card: 300, Skew: 1.2},
	{Name: "version", Type: TypeInt32, Min: 1, Max: 3},
	{Name: "reconciled", Type: TypeBool},
	{Name: "tags", Type: TypeString, Source: SourceChoice, Repeat: list(1, 0.6, 1, 6), Optional: 0.5,
		Symbols: []string{"urgent", "manual", "bulk", "intercompany", "payroll", "vendor", "refund", "review", "high_value", "cross_border"}},
	{Name: "fees", Type: TypeRecord, TypeName: "Fee", Repeat: list(1, 0.5, 1, 3), Optional: 0.6, Fields: []Field{
		{Name: "kind", Type: TypeEnum, TypeName: "FeeKind", Symbols: []string{"WIRE_FEE", "FX_FEE", "SERVICE_FEE"}, Weights: []float64{60, 25, 15}},
		{Name: "amount", Type: TypeMoney, Fee: true},
	}},
	{Name: "metadata", Type: TypeRecord, TypeName: "KeyValue", Repeat: list(12, 1.0, 1, 1000), Optional: 0.97, Fields: []Field{
		{Name: "key", Type: TypeString, Source: SourceDict, Dict: DictWord, Card: 60, Skew: 1.0},
		{Name: "value", Type: TypeString, Source: SourceText, Len: Dist{Median: 2, Sigma: 0.8, Min: 1, Max: 60}},
	}},
}}

// BankLine is a bank statement line, the right side of a run.
var BankLine = &Schema{Name: "BankLine", Fields: []Field{
	{Name: "id", Type: TypeIdentity},
	{Name: "bank_ref", Type: TypeString, Source: SourceKey},
	{Name: "account", Type: TypeString, Source: SourceDict, Dict: DictIBAN, Card: 2000, Skew: 1.1},
	{Name: "booked_at", Type: TypeTimestamp, Window: 30 * day},
	{Name: "value_date", Type: TypeDate, Lag: 2},
	{Name: "amount", Type: TypeMoney, Median: 12000, Sigma: 1.6, Signed: true},
	{Name: "bank_tx_code", Type: TypeString, Source: SourceChoice,
		Symbols: []string{"PMNT-ICDT-ESCT", "PMNT-RCDT-ESCT", "PMNT-IDDT-ESDD", "PMNT-CCRD-POSD", "ACMT-MCOP-CHRG", "PMNT-ICDT-XBCT", "PMNT-RCDT-XBCT", "PMNT-ICHQ-CCHQ", "LDAS-NTAV-INTR", "PMNT-MCRD-DAJT"},
		Weights: []float64{30, 25, 12, 10, 6, 5, 5, 3, 2, 2}},
	{Name: "counterparty", Type: TypeRecord, TypeName: "BankParty", Fields: []Field{
		{Name: "name", Type: TypeString, Source: SourceDict, Dict: DictName, Card: 5000, Skew: 1.0},
		{Name: "bic", Type: TypeString, Source: SourceDict, Dict: DictBIC, Card: 500, Skew: 1.1, Optional: 0.2},
	}},
	{Name: "remittance_info", Type: TypeString, Source: SourceText, Len: Dist{Median: 5, Sigma: 0.6, Min: 1, Max: 140}, Optional: 0.2},
	{Name: "balance_after", Type: TypeMoney, Median: 25000000, Sigma: 1.2},
}}

// CardSettlement is a card network's settlement record: mostly numbers.
var CardSettlement = &Schema{Name: "CardSettlement", Fields: []Field{
	{Name: "id", Type: TypeIdentity},
	{Name: "key", Type: TypeString, Source: SourceKey},
	{Name: "merchant_id", Type: TypeInt64, Min: 100000, Max: 999999},
	{Name: "terminal_id", Type: TypeString, Source: SourceDict, Dict: DictTerminal, Card: 20000, Skew: 0.9},
	{Name: "card_bin", Type: TypeInt32, Min: 400000, Max: 559999},
	{Name: "card_last4", Type: TypeString, Source: SourceToken, Alphabet: digits, Len: Dist{Median: 4, Min: 4, Max: 4}},
	{Name: "auth_code", Type: TypeString, Source: SourceToken, Alphabet: alnum, Len: Dist{Median: 6, Min: 6, Max: 6}},
	{Name: "amount", Type: TypeMoney, Median: 4500, Sigma: 1.4},
	{Name: "interchange_fee", Type: TypeMoney, Fee: true},
	{Name: "txn_time", Type: TypeTimestamp, Window: 30 * day},
	{Name: "settle_date", Type: TypeDate, Lag: 3},
	{Name: "mcc", Type: TypeInt32, Ints: []int64{5411, 5812, 5541, 5999, 4121, 5732, 7011, 4111, 5311, 5691, 5912, 7372}, Weights: []float64{20, 15, 12, 10, 8, 7, 6, 6, 5, 4, 4, 3}},
	{Name: "entry_mode", Type: TypeEnum, TypeName: "EntryMode", Symbols: []string{"CHIP", "CONTACTLESS", "ECOM", "SWIPE", "MANUAL"}, Weights: []float64{25, 40, 30, 4, 1}},
	{Name: "network", Type: TypeEnum, TypeName: "Network", Symbols: []string{"VISA", "MASTERCARD", "AMEX", "DISCOVER"}, Weights: []float64{55, 35, 7, 3}},
	{Name: "arn", Type: TypeString, Source: SourceToken, Alphabet: digits, Len: Dist{Median: 23, Min: 23, Max: 23}},
}}

// Envelope is a value a client encrypted: a key ID, a nonce and the
// ciphertext. It doesn't compress.
var Envelope = &Schema{Name: "Envelope", Fields: []Field{
	{Name: "id", Type: TypeIdentity},
	{Name: "key_id", Type: TypeString, Source: SourceChoice, Symbols: []string{
		"arn:aws:kms:eu-west-1:111122223333:key/1f6b0c0e-4a52-4d7e-9b1a-6c2f0e9d8a71",
		"arn:aws:kms:eu-west-1:111122223333:key/8d2e5a4b-0c1f-4e3a-a7b6-5f9c3d2e1b08"}, Weights: []float64{95, 5}},
	{Name: "nonce", Type: TypeBytes, Len: Dist{Median: 12, Min: 12, Max: 12}},
	{Name: "ciphertext", Type: TypeBytes, Len: Dist{Median: 400, Sigma: 0.6, Min: 16, Max: 16384}},
}}

// Tiny is a minimal record, for per-record overhead.
var Tiny = &Schema{Name: "Tiny", Fields: []Field{
	{Name: "id", Type: TypeIdentity},
	{Name: "amount_minor", Type: TypeSInt64, Median: 4500, Sigma: 1.4, Signed: true},
	{Name: "status", Type: TypeEnum, TypeName: "TinyStatus", Symbols: []string{"OK", "PENDING", "FAILED"}, Weights: []float64{90, 8, 2}},
	{Name: "value_date", Type: TypeDate},
}}

// Profiles returns the built-in schemas: Payment, BankLine,
// CardSettlement, Envelope and Tiny.
func Profiles() []*Schema {
	return []*Schema{Payment, BankLine, CardSettlement, Envelope, Tiny}
}

// Profile returns the built-in schema named name, ignoring case, or nil.
func Profile(name string) *Schema {
	for _, s := range Profiles() {
		if strings.EqualFold(s.Name, name) {
			return s
		}
	}
	return nil
}
