# Fast Messenger Pro v14 — Realtime Database setup

This package uses Firebase Realtime Database for users, friends, friend requests, groups, messages, typing/presence and call signalling. The old Firestore client has been removed.

## Firebase project

Project: `fast-massage-3ac80`

Realtime Database URL: `https://fast-massage-3ac80-default-rtdb.firebaseio.com`

## Rules

Firebase Console → Realtime Database → Rules → replace the existing rules with `database.rules.json` and Publish.

## Important

The browser still uses Firebase Authentication for Google sign-in. Realtime Database is the application data store. The compatibility layer inside `assets/js/app.js` preserves the app's existing collection/document logic while translating every operation to RTDB.

Calls still require a valid Agora App ID. If Agora token authentication is enabled in your Agora project, a server-generated token is required; the Firebase database rules cannot generate Agora tokens.

## Friend realtime fix

The friend directory, incoming/outgoing friend requests, and mirrored friendship records now use Firebase Realtime Database child events (`child_added`, `child_changed`, `child_removed`) so newly created accounts and friend-request changes appear in Find people/Requests/Friends without a manual refresh. Accepting a request uses one RTDB multi-location update to create both friend records and mark the request accepted together.
