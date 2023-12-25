<!--
SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>

SPDX-License-Identifier: CC-BY-SA-2.0
-->

# Licensing in Thorium

Thorium itself is released under the terms of the
[MIT License](https://spdx.org/licenses/MIT.html),
as documented in [the LICENSE file](../../LICENSE) of this project.

Thorium uses [SPDX License IDs](https://spdx.dev/learn/handling-license-info/)
internally to identify  license and copyright information, and complies with
FSF Europe's [REUSE Specification](https://reuse.software/spec/) for software
licensing. For example, Kotlin files begin with a comment block looking
like

```kotlin
// SPDX-FileCopyrightText: 2023 Dani Sweet <thorium@djsweet.name>
//
// SPDX-License-Identifier: MIT
```

This establishes the ownership and terms of distribution in a machine-readable
way.

Files that cannot include comments, such as JAR files, have their licenses
described in a separate `.license` file. For example,
`gradle/wrapper/gradle-wrapper.jar` contains licensing information in
`gradle/wrapper/gradle-wrapper.jar.license`:

```
SPDX-FileCopyrightText: 2015-2023 the original authors
SPDX-License-Identifier: Apache-2.0
```

## License Tooling

Thorium development uses FSF Europe's
[reuse tool](https://git.fsfe.org/reuse/tool) to automate compliance
and compliance verification. As a quickstart for using `reuse`:

1. Install `pipx` through your preferred package manager.
2. Set up your `PATH` to include `~/.local/bin`. 
3. `pipx install reuse`
4. Run `reuse lint` in the top-level path.

Most of the usage of `reuse` in this project involves the following commands:

- `reuse lint` will tell you which files are not in compliance with the REUSE
  specification.
- `reuse annotate --license <SPDX License Identifier> --copyright "<Copyright string>" FILE ...`
  will update the given `FILE`s copyright headers.
  - `reuse annotate ... --force-dot-license FILE` will generate a `.license`
    file for the given FILE instead of attempting to update the file.
- `reuse download <SPDX License Identifier>` will download a copy of the
  given license into the `LICENSES` directory.
  