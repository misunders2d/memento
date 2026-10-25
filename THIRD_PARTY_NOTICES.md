# Third-party notices

The service image bundles the unmodified Qdrant 1.20.0 executable from the official
`qdrant/qdrant@sha256:9bfc89f9914fc14876a88c05270685921a55b8877b254a98bcc1cc2f0a424cd2` image.
Qdrant is distributed under the Apache License 2.0; its full license is retained in
`src/memento/QDRANT_LICENSE` and `/usr/share/doc/memento-qdrant/LICENSE` in the image.
This bundle pins the Linux x86_64 executable; ARM is not supported by this pin.
The original project is https://github.com/qdrant/qdrant.

The indexing units, search scoring and many parameters in this project are ported (re-implemented in Python) from
[DigUp](https://github.com/ARahim3/DigUp), which is distributed under the following license:

```
MIT License

Copyright (c) 2026 Abdur Rahim

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

The memory viewer under `src/memento/ui/vendor/` ships these unmodified or bundled third-party builds:

- `sigma.min.js`: [sigma](https://www.sigmajs.org) 3.0.3 (`dist/sigma.min.js`), Copyright (C) 2013-2025, Alexis Jacomy, Guillaume Plique, Benoît Simard.
- `graphology.umd.min.js`: [graphology](https://graphology.github.io) 0.26.0 (`dist/graphology.umd.min.js`), Copyright (c) 2016-2021 Guillaume Plique (Yomguithereal).
- `forceatlas2.min.js`: graphology-layout-forceatlas2 0.10.1 with graphology-utils 2.5.2, Copyright (c) 2016-2021 and 2017-2021 Guillaume Plique (Yomguithereal), bundled with
  `esbuild node_modules/graphology-layout-forceatlas2/index.js --bundle --minify --format=iife --global-name=forceAtlas2` (esbuild 0.25.12).

Each is distributed under the MIT License:

```
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
