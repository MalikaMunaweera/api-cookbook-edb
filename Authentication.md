The Shortcut API uses token-based authentication. Tokens are linked to a specific Shortcut user. This makes it easy for you to make updates to work via the API and keep your team in the loop about who made changes.

To generate an API token, go to https://app.shortcut.com/settings/account/api-tokens. You can also access this from your user Settings.

We recommend saving this token as an environment variable. This helps keep your token secure and makes working with the API easier.

`export SHORTCUT_API_TOKEN="YOUR API TOKEN HERE"`

All of the scripts in the Shortcut API Cookbook assume that you have set an environment variable `SHORTCUT_API_TOKEN` to hold your token. Not sure how to do that? We walk you through it!
- [Mac](https://github.com/useshortcut/api-cookbook/blob/master/set-up-instructions.md#setting-your-environment-variable).
- [Windows](https://github.com/useshortcut/api-cookbook/blob/master/windows-set-up-instructions.md#setting-your-environment-variable)

### Troubleshooting
Requests made with a missing or invalid token will get a 401 Unauthorized response. Make sure your token is correct, and that your environment variable is correct.

If you're writing your own scripts, using a different language or a different library, be sure you're adding the token to the URL and not passing the token as an authentication header.

All requests must be made over HTTPS. Requests made over HTTP will fail to connect and timeout.

### Security reminders
Tokens provide **complete** access to your Shortcut account; keep them secure.

Don’t paste them into your source code, even for test accounts. Take the time to set up an environment variable and get in the habit of using it.

For security reasons, we will immediately invalidate any tokens we find have been made public.
