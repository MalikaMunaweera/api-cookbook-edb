In this guide, we'll take you through all the things you might need to set up and run the Shortcut API Cookbook scripts.

You'll need Administrator access to your Mac (Linux ok, [Windows use this guide](link)) and an active internet connection to get everything set up.

Plan for up to 30 minutes to get everything installed - it may be much faster though!

We'll cover:
- [Opening a command line interface](#opening-a-command-line-interface)
- [Checking what version of Python you have installed](#checking-what-version-of-python-you-have-installed)
- [Installing Python 3](#installing-python-3)
- [Setting your Environment Variable](#setting-your-environment-variable)
- [Setting up and using a virtual environment](#setting-up-and-using-a-virtual-environment)
- [Installing the Requests library](#installing-the-requests-library)
- [Downloading the Shortcut API Cookbook](#downloading-the-shortcut-api-cookbook)

***

#### Opening a command line interface

The command line also gets called cmd, CLI, prompt, console or terminal - all of these are referring to a text-based way to work with files on your computer.

We'll use a few commands as we get things set up and run Cookbook scripts. Don't worry, all commands you'll use will be included in the instructions.

We won't go into detail about all the things you can do with the command line, but command line cheat sheets are a great way to get familiar with the commonly used commands to interact with your system.

Different operating systems have different command line interfaces.

**On Mac:**
Open Applications, then click Utilities. Click the Terminal app to open the command line.

**On Linux:**
Check in Applications in either the Accessories folder or the Applications folder for the Terminal app.
If you're running a distro that doesn't have Terminal in one of those places, you probably know how to launch it. :)

Once you have a command line window open, it's time to check your current Python version.

#### Checking what version of Python you have installed

All of the Cookbook scripts use Python 3, so we need to make sure you have Python 3 installed.

Open your [command line](#opening-a-command-line-interface)
Type `python3 --version` and press Enter/Return

You'll see the default version of Python 3 that is installed on your system. Any version of Python 3 above 3.6.x will work well.

If nothing is present, you'll need to install Python 3.

#### Installing Python 3

If you already use a package manager like Homebrew (Mac), install Python 3 with your package manager.

Otherwise, grab the appropriate download for your operating system from the [Python Software Foundation](https://www.python.org/downloads/).


***
#### Setting your Environment Variable
***

All access to the Shortcut API is [token based](https://github.com/useshortcut/api-cookbook/blob/master/Authentication.md). Your token will be a string of characters that is generated in the Shortcut UI. Unlike a password, these tokens cannot be changed (just deleted). Each time that a script you are using needs to access the API, your token must be included in the request.
While you can manually add your token to any script you use, we recommend saving this token as an environment variable. This helps keep your token secure and makes working with the API easier.

Get your [Shortcut API token](https://app.shortcut.com/settings/account/api-tokens) from the Shortcut UI.

If you're going to use the API often, we recommend setting your API token as a system variable as this will save you time.

On Mac:

We're assuming you're using zsh since it's the default user shell. If you're using a different shell (like bash or tcsh), and aren't sure how to set a variable for your setup, check the documentation for the shell you're using.

If you have other system variables set, like AWS credentials, and aren't sure where those credentials are, you may want to check with a technical lead on your team so you can set your Shortcut API variable in the same file.

You'll use Terminal to set your Shortcut API token as an environment variable in ~/.bash_profile :

1. Type `cd ~` and press Return to go to your home directory
2. Type `nano .zshenv` and press Return to open .zshenv in the nano text editor (Feel free to use your preferred text editor instead)
3. Add the line `export SHORTCUT_API_TOKEN='YOUR_TOKEN_VALUE'` to the file in nano, where YOUR_TOKEN_VALUE is your actual API token from Shortcut. Keep the single quotes around the token.
4. Press ⌃O (control key and the letter 'o') then return to save the changes - this is like the ⌘S (Command-S )to save in most programs.
5. Press ⌃X (control key and letter 'x') to exit.
6. Close your Terminal window - this is necessary for the change to be applied!
7. Open new Terminal window and test your environment variable by typing:
`echo $SHORTCUT_API_TOKEN`

The response for `echo $SHORTCUT_API_TOKEN` should contain your token. If you see a blank line there are two common reasons:
1. If you haven't closed and reopened your shell the change won't have been picked up. Close your shell window and reopen before trying again.
2. If you are running MacOS Mojave (10.14) or earlier your default shell will be bash and you will need to use `nano .bash_profile` instead of `nano .zshenv`. 

If you are just testing things out, or don't need to regularly access the Shortcut API, you can set a temporary environment variable.

In your command line window, type `export SHORTCUT_API_TOKEN='YOUR_TOKEN_VALUE'` where YOUR_TOKEN_VALUE is your actual API token from Shortcut. Keep the single quotes around the token.

When you close your command line window, this variable will be removed, and you'll need to add it again every time to you want to use something in the Cookbook.


#### Setting up and using a virtual environment

Setting up a virtual environment is not 100% necessary if you don't regularly use a lot of Python, but it is highly recommended. It's a good practice that can save you trouble in the future and can make the next step easier!

A virtual environment will keep installed Python packages separate from your system environment (everything on your computer that's not in a virtual environment). This helps keep all of your code dependencies separate and makes it possible to work with code that has different and possibly incompatible dependencies.

Without a virtual environment, you could end up with dependencies interfering with each other - which is a real pain to debug. Be kind to your future self and set up a virtual environment for working with this Cookbook. We'll help guide you through it!

If you're regularly working with Python 2, you're probably already using [virtualenv](hhttps://virtualenv.pypa.io/en/latest/). We'll assume you're comfortable working with that without our guidance. :)

The remainder of these instructions will use [venv](https://docs.python.org/3/library/venv.html) to create a virtual environment.

Let's get started!

1. Choose the location where you'll keep the Cookbook scripts. We'll use a folder named `ShortcutCookbook` in our home directory, but you can make a folder anywhere that makes sense for you, and rename it as well.
In your [command line](#opening-a-command-line-interface) window type:
`mkdir ShortcutCookbook`
and press Enter/Return.

2. We want to create the virtual environment in the directory(folder) that we just created, so we need to navigate to inside that folder.
In your command line window type:
`cd ShortcutCookbook`
and press Enter/Return.

3. Make a virtual environment.
We'll make one called `cookbook`. You can use any name, but be sure to avoid spaces, accents, or special characters and keep it all lowercase.
In your command line window type:
`python3 -m venv cookbook`

4. Start your virtual environment.
In your command line window type:
`source cookbook/bin/activate`

When your virtual environment is active, you'll see `(cookbook)` at the beginning of your command line prompt.

Now we're ready to install Requests!

#### Installing the Requests library

We'll need the [Requests Library](http://docs.python-requests.org/en/master/) which you can install with pip.

In Terminal type `pip install requests` and press Enter/Return.

If you see a message about needing to update pip, follow the instructions in the command line window.

That covers the last installation requirement. Let's go get the Cookbook!

#### Downloading the Shortcut API Cookbook

There are a few options around how to get the Shortcut API Cookbook scripts onto your computer, so that you can run them on your device. Since we set up the Requests library in a virtual environment, we'll want to put the Cookbook in that same environment.

If you've been following along in your command line window, you should still be in the ShortcutCookbook folder. If not, you'll need to navigate to that folder or the folder you're using to hold the Cookbook scripts.

Use `cd` to change directories (aka folders). For example, when we made the ShortcutCookbook folder in our home directory, we then navigated into that folder with `cd ShortcutCookbook`.

Once you're in that folder in your command line window, paste `git clone https://github.com/useshortcut/api-cookbook.git` and press Enter/Return

If you'd prefer to get the Cookbook without using the command line, or you don't have git installed, you can [download a zipped folder from GitHub's UI](https://help.github.com/en/articles/cloning-a-repository). You'll need to unzip the folder and then move all of the contents in to the location that you set up your virtual environment.

To run each script, navigate to into the downloaded API cookbooks with `cd api-cookbook` then switch to the folder for the specific script.

Once you have your API token set as an environment variable, the scripts in these folders can be run without modification
`cd change-label`
`cd kanban-metrics`

The script in this folder requires editing to be able to send Stories to your Slack workspace.
`cd stories-to-slack`

Make sure your [virtual environment](#setting-up-and-using-a-virtual-environment) is active. Then type `python name_of_the_script.py` and press Return/Enter to run the script. Make sure you've replaced name_of_the_script.py with the actual file name.
