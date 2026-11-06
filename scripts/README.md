go mod init tiledl && go build -o tiledl .

./tiledl -url "https://tile.example.com/{z}/{x}/{y}.png" \
         -bbox "-122.52,37.70,-122.35,37.83" \
         -zoom 10-14 -out ./tiles -workers 8




go run downloader.go \
        -url "https://tiles.thestefanproblem.rocks/api/v1/provider/mapbox_dark/{z}/{x}/{y}.png" \
        -bbox "-125.0,45.0,-120.0,46.0" \
        -zoom 10-12 \
        -out ./tiles \
        -workers 8