//
// CsvWriter.hpp
// Header-only CSV writer for TpccGenerator
//
// Usage:
//   csv::CsvWriter w(filename);
//   w << value1 << csv::Precision(4) << float_val << csv::endl;
//
// Rules:
//   - Values are separated by commas automatically
//   - csv::Precision(n)  sets decimal places for the NEXT float/double
//   - csv::endl          ends the current row (writes newline, resets state)
//   - array<char,N>      written as a quoted string (length = strnlen up to N,
//                        safe even when buffer is not null-terminated)
//   - char*              written as a quoted string (trimmed at first '\0')
//

#pragma once

#include <array>
#include <cstdint>
#include <cstring>   // strnlen — required for bounded array<char,N> read
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

namespace csv {

// ── Manipulators ─────────────────────────────────────────────────────────────

struct Precision {
    int digits;
    explicit Precision(int d) : digits(d) {}
};

// Sentinel type for csv::endl
struct EndlType {};
static const EndlType endl;

// ── CsvWriter ─────────────────────────────────────────────────────────────────

class CsvWriter {
public:
    explicit CsvWriter(const std::string& filename)
        : file_(filename), first_on_line_(true), next_precision_(-1)
    {
        if (!file_.is_open())
            throw std::runtime_error("[CsvWriter] Cannot open file: " + filename);
    }

    ~CsvWriter() {
        if (file_.is_open()) file_.close();
    }

    // ── Precision manipulator ─────────────────────────────────────────────
    CsvWriter& operator<<(const Precision& p) {
        next_precision_ = p.digits;
        return *this;
    }

    // ── endl manipulator ─────────────────────────────────────────────────
    CsvWriter& operator<<(const EndlType&) {
        file_ << "\n";
        first_on_line_ = true;
        next_precision_ = -1;
        return *this;
    }

    // ── Integer types ─────────────────────────────────────────────────────
    CsvWriter& operator<<(int8_t   v) { write_int(v); return *this; }
    CsvWriter& operator<<(int16_t  v) { write_int(v); return *this; }
    CsvWriter& operator<<(int32_t  v) { write_int(v); return *this; }
    CsvWriter& operator<<(int64_t  v) { write_int(v); return *this; }
    CsvWriter& operator<<(uint8_t  v) { write_int(v); return *this; }
    CsvWriter& operator<<(uint16_t v) { write_int(v); return *this; }
    CsvWriter& operator<<(uint32_t v) { write_int(v); return *this; }
    CsvWriter& operator<<(uint64_t v) { write_int(v); return *this; }

    // ── Floating-point types ──────────────────────────────────────────────
    CsvWriter& operator<<(float v) {
        write_sep();
        int prec = (next_precision_ >= 0) ? next_precision_ : 6;
        file_ << std::fixed << std::setprecision(prec) << v;
        next_precision_ = -1;
        return *this;
    }

    CsvWriter& operator<<(double v) {
        write_sep();
        int prec = (next_precision_ >= 0) ? next_precision_ : 6;
        file_ << std::fixed << std::setprecision(prec) << v;
        next_precision_ = -1;
        return *this;
    }

    // ── std::string ───────────────────────────────────────────────────────
    CsvWriter& operator<<(const std::string& s) {
        write_sep();
        write_quoted(s.c_str());
        return *this;
    }

    // ── const char* ───────────────────────────────────────────────────────
    CsvWriter& operator<<(const char* s) {
        write_sep();
        write_quoted(s);
        return *this;
    }

    // ── std::array<char, N> ───────────────────────────────────────────────
    // Handles all fixed-size char arrays used in TpccGenerator.
    //
    // FIX (4/20/26): Previously this called write_quoted(arr.data()), which
    // scans the pointer byte-by-byte until it hits '\0'. That contract is
    // BROKEN by TpccGenerator's makeAlphaString(N, N, buf): when min == max,
    // the function writes exactly N alphanumerics into the buffer and does
    // NOT append '\0' (there's no room — the array is sized to exactly N).
    // This applies to every stock.s_dist_01..10, ol_dist_info, c_state,
    // c_middle, etc. The old loop would then run past the buffer into
    // adjacent stack memory, emitting whatever happened to be there —
    // including '\n' and non-ASCII bytes like 0xA7 — into the CSV. That
    // corrupted rows, broke line-based CSV readers, blew up bin counts in
    // the partitioning layer (500K fake bins for s_quantity instead of 91),
    // and triggered OOM during ORAM tree sizing.
    //
    // The fix: use strnlen to cap the scan at N. strnlen returns the length
    // of the null-terminated prefix, or N if no '\0' appears in the first
    // N bytes. Then write exactly that many bytes — no overrun possible.
    //
    // Semantics preserved for every existing use site:
    //   • variable-length fields (c_data, s_data, i_data, ...)
    //       — filled with len < N chars and a '\0' at dest[len] → strnlen
    //         returns len, we write len bytes. Same as before.
    //   • fixed-length fields (s_dist_01..10, ol_dist_info, c_state, ...)
    //       — filled with exactly N chars, no '\0' → strnlen returns N,
    //         we write all N bytes. Previously would overrun.
    template<std::size_t N>
    CsvWriter& operator<<(const std::array<char, N>& arr) {
        write_sep();
        const std::size_t len = ::strnlen(arr.data(), N);
        write_quoted_bounded(arr.data(), len);
        return *this;
    }

private:
    std::ofstream file_;
    bool          first_on_line_;
    int           next_precision_;   // -1 = use default

    void write_sep() {
        if (!first_on_line_) file_ << ",";
        first_on_line_ = false;
    }

    // Write a C-string, quoting it and escaping internal quotes
    void write_quoted(const char* s) {
        file_ << "\"";
        if (s) {
            for (const char* p = s; *p != '\0'; ++p) {
                if (*p == '"') file_ << "\"\"";   // escape double-quote
                else           file_ << *p;
            }
        }
        file_ << "\"";
    }

    // Bounded variant used by the std::array<char, N> overload.
    // Writes exactly `len` bytes; does NOT stop at embedded '\0'.
    // Callers are responsible for computing `len` safely (e.g. via strnlen).
    void write_quoted_bounded(const char* s, std::size_t len) {
        file_ << "\"";
        if (s) {
            for (std::size_t i = 0; i < len; ++i) {
                if (s[i] == '"') file_ << "\"\"";   // escape double-quote
                else             file_ << s[i];
            }
        }
        file_ << "\"";
    }

    template<typename T>
    void write_int(T v) {
        write_sep();
        file_ << v;
        // Integers never consume next_precision_
    }
};

} // namespace csv