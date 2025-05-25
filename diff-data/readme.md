### install fyne

https://docs.fyne.io/started/

The MSYS2 platform is the recommended approach for working on Windows. Proceed as follows:

Install MSYS2 from msys2.org
Once installed do not use the MSYS terminal that opens
Open “MSYS2 MinGW 64-bit” from the start menu
Execute the following commands (if asked for install options be sure to choose “all”):

 $ pacman -Syu
 $ pacman -S git mingw-w64-x86_64-toolchain mingw-w64-x86_64-go

You will need to add ~/Go/bin to your $PATH, for MSYS2 you can paste the following command into your terminal:

 $ echo "export PATH=\$PATH:~/Go/bin" >> ~/.bashrc

For the compiler to work on other terminals you will need to set up the windows %PATH% variable to find these tools. Go to the “Edit the system environment variables” control panel, tap “Advanced” and add “C:\msys64\mingw64\bin” to the Path list.
Downloading

$ go get fyne.io/fyne/v2@latest
$ go install fyne.io/tools/cmd/fyne@latest
If you are unsure of how Go modules work, consider reading Tutorial: Create a Go module.


### build app

go build -ldflags -H=windowsgui
