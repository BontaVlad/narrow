#!/bin/bash
echo "=== Finding Arrow libraries ==="

# Method 1: pkg-config (Core Arrow)
echo "Arrow libdir (pkg-config):"
pkg-config --variable=libdir arrow 2>/dev/null || echo "Not found"

# --- NEW: Check arrow-glib and version compatibility ---
echo -e "\n=== Checking arrow-glib (C-GLib Bindings) ==="
if pkg-config --exists arrow-glib; then
    GLIB_VERSION=$(pkg-config --modversion arrow-glib)
    GLIB_LIBDIR=$(pkg-config --variable=libdir arrow-glib)
    echo "Found arrow-glib version: $GLIB_VERSION"
    echo "Location: $GLIB_LIBDIR"

    # Check for version 22.0 compatibility
    if pkg-config --atleast-version=22.0 arrow-glib; then
        echo "✅ Version is compatible (22.0 or newer)"
    else
        echo "❌ Version $GLIB_VERSION is older than 22.0"
    fi
else
    echo "❌ arrow-glib NOT FOUND via pkg-config"
fi

# Method 2: ldconfig
echo -e "\nArrow libraries (ldconfig):"
ldconfig -p | grep -E "arrow|parquet"

# Method 3: Direct search
echo -e "\nSearching /usr/lib for Arrow & GLib:"
find /usr/lib -name "libarrow*.so*" 2>/dev/null

# Check if specific libraries exist (Updated list)
echo -e "\nChecking specific libraries:"
LIBS=("libarrow.so" "libarrow_acero.so" "libarrow_compute.so" "libarrow-glib.so")

for lib in "${LIBS[@]}"; do
    path=$(ldconfig -p | grep "$lib" | awk '{print $NF}' | head -1)
    if [ -n "$path" ]; then
        echo "✓ $lib -> $path"
        ls -la "$path"
    else
        echo "✗ $lib NOT FOUND"
    fi
done
