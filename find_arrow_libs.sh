#!/bin/bash
echo "=== Finding Arrow libraries ==="

# Method 1: pkg-config
echo "Arrow libdir (pkg-config):"
pkg-config --variable=libdir arrow

# Method 2: ldconfig
echo -e "\nArrow libraries (ldconfig):"
ldconfig -p | grep arrow

# Method 3: Direct search
echo -e "\nSearching /usr/lib:"
find /usr/lib -name "libarrow*.so*" 2>/dev/null

echo -e "\nSearching /usr/local/lib:"
find /usr/local/lib -name "libarrow*.so*" 2>/dev/null

# Check if libraries exist
echo -e "\nChecking specific libraries:"
for lib in libarrow.so libarrow_acero.so libarrow_compute.so; do
    path=$(ldconfig -p | grep "$lib" | awk '{print $NF}' | head -1)
    if [ -n "$path" ]; then
        echo "✓ $lib -> $path"
        ls -la "$path"
    else
        echo "✗ $lib NOT FOUND"
    fi
done
