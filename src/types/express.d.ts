// Ambient augmentation of the Express `Request` object.
//
// `user` is attached by the auth middleware (see `middleware/requireUserId.ts`,
// which sets `req.user = { id }`). `services` is the request-scoped dependency
// container the admin force-score handler expects; both are optional because
// they are only present after the relevant middleware/DI has run.
import "express";

declare global {
  namespace Express {
    interface Request {
      user?: { id: string };
      services?: {
        scoring: {
          enqueue(input: { callId: string; userId: string }): Promise<string>;
        };
      };
    }
  }
}

export {};
