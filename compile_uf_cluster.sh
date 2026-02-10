#!/bin/bash

# Get the absolute path of the project root
export IMAGE="burstcomputing/runtime-rust-burst:latest"

echo "🚀 Starting compilation of Union Find action using cluster-identical environment..."

docker run --rm --entrypoint="" \
    -v "./ow-uf":/tmp/input_actions \
    -v "../burst-communication-middleware":/tmp/input_middleware \
    "$IMAGE" \
    /bin/bash -c "
        # 1. Prepare isolated source folders (avoiding mount point busy errors)
        cp -r /tmp/input_actions /tmp/actions_src
        cp -r /tmp/input_middleware /tmp/middleware_src
        
        # 2. Replace the container's internal middleware with our local version
        rm -rf /usr/src/burst-communication-middleware
        mv /tmp/middleware_src /usr/src/burst-communication-middleware
        
        # 3. Compile using the image's internal script
        # This script automatically uses the golden Cargo.lock and handles the proxy wrapper
        python3 /usr/bin/compile.py main /tmp/actions_src /tmp
        
        # 4. Copy the resulting binary back to the mount
        cp /tmp/exec /tmp/input_actions/exec_cluster
    "

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful!"
    mkdir -p "./ow-uf/bin"
    cp "./ow-uf/exec_cluster" "./ow-uf/bin/exec"
    chmod +x "./ow-uf/bin/exec"
    rm -f "./ow-uf/exec_cluster"
    zip -j "./unionfind.zip" "./ow-uf/bin/exec"
    echo "📦 Zip is ready"
else
    echo "❌ Compilation failed."
    exit 1
fi
