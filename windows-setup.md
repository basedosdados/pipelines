# Windows Native Setup with uv

- Install Git
- Install VSCode `winget install --id=Microsoft.VisualStudioCode -e`
- R Lang (required by rpy2 package) `winget install -e --id RProject.R`
- Microsoft Visual C++ 14.0 or greater is required. Get it with "Microsoft C++ Build Tools"
  - Required by psutil (prefect -> distributed -> psutil)
  - `winget install Microsoft.VisualStudio.2022.BuildTools --force --override "--wait --passive --add Microsoft.VisualStudio.Component.VC.Tools.x86.x64 --add Microsoft.VisualStudio.Component.Windows11SDK.22621"`

## Enable venv

- Open powershell as admin and run `set-executionpolicy remotesigned`
  - https://learn.microsoft.com/en-us/previous-versions//bb613481(v=vs.85)?redirectedfrom=MSDN

```sh
.venv\Scripts\activate
```

Se tiver algum erro:

- Enable execution of PowerShell scripts https://superuser.com/questions/106360/how-to-enable-execution-of-powershell-scripts
