{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards #-}

module UnliftIO.Retry
    ( Backoff (..)
    , recovering
    , BackoffAtMost(..)
    , backoffAtMost
    , BackoffLimitDelay(..)
    , backoffLimitDelay
    , ConstantBackoff (..)
    , constantBackoff
    , ExponentialBackoff (..)
    , exponentialBackoff
    ) where

import Control.Concurrent
import Control.Monad.IO.Unlift
import GHC.Generics
import UnliftIO.Exception

class Monad m => Backoff m backoff where
    getNext :: backoff -> m (Maybe (Int, backoff))

chainBackoff :: (chainBackoff -> backoff) -> Maybe (Int, chainBackoff) -> Maybe (Int, backoff)
chainBackoff = fmap . fmap

data BackoffAtMost backoff =
    BackoffAtMost Int Int backoff
    deriving (Show, Eq, Ord, Generic)
instance (Monad m, Backoff m nextBackoff) => Backoff m (BackoffAtMost nextBackoff) where
    getNext (BackoffAtMost atMost cur nextBackoff)
        | atMost - 1 > cur = chainBackoff (BackoffAtMost atMost (cur + 1)) <$> getNext nextBackoff
        | otherwise = pure Nothing

backoffAtMost :: Int -> backoff -> BackoffAtMost backoff
backoffAtMost atMost = BackoffAtMost atMost 0

data BackoffLimitDelay backoff =
    BackoffLimitDelay Int backoff
    deriving (Show, Eq, Ord, Generic)
instance (Monad m, Backoff m chainedBackoff) => Backoff m (BackoffLimitDelay chainedBackoff) where
    getNext (BackoffLimitDelay maximumDelay chainedBackoff) =
        getNext chainedBackoff >>= \case
            Just (rawWait, nextChainedBackoff) ->
                pure $ Just (min rawWait maximumDelay, BackoffLimitDelay maximumDelay nextChainedBackoff)
            Nothing -> pure Nothing

backoffLimitDelay :: Int -> backoff -> BackoffLimitDelay backoff
backoffLimitDelay = BackoffLimitDelay

newtype ConstantBackoff =
    ConstantBackoff Int
    deriving (Show, Eq, Ord, Generic)
instance Monad m => Backoff m ConstantBackoff where
    getNext b@(ConstantBackoff wait) = pure $ Just (wait, b)

constantBackoff :: Int -> ConstantBackoff
constantBackoff = ConstantBackoff

data ExponentialBackoff =
    ExponentialBackoff
        { factor :: Int
        , tries :: Int
        }
    deriving (Show, Eq, Ord, Generic)
instance Monad m => Backoff m ExponentialBackoff where
    getNext ExponentialBackoff {..} = pure $ Just (factor * 2 ^ tries, ExponentialBackoff factor (tries + 1))

exponentialBackoff :: Int -> ExponentialBackoff
exponentialBackoff factor = ExponentialBackoff factor 0

recovering ::
       (Backoff m backoff, MonadUnliftIO m)
    => backoff -- ^
    -> [Int -> Handler m Bool] -- ^ handlers
    -> (Int -> m a) -- ^ actions
    -> m a
recovering backoff handlers act = loop backoff 0
  where
    loop currentBackoff tries = act tries `catch` handleError
      where
        handleError e = foldr tryHandler (throwIO e) handlers
          where
            tryHandler hndl res
                | Handler h <- hndl tries =
                    case fromException e of
                        Just e' -> h e' >>= retry
                        Nothing -> res
            retry True = do
                applyBackoffDelay currentBackoff >>= \case
                    Just nextBackoff -> loop nextBackoff (tries + 1)
                    Nothing -> throwIO e
            retry False = throwIO e

applyBackoffDelay ::
       (Backoff m backoff, MonadUnliftIO m)
    => backoff -- ^
    -> m (Maybe backoff)
applyBackoffDelay backoff = do
    getNext backoff >>= \case
        Nothing -> return Nothing
        Just (delay, nextBackoff) -> do
            liftIO $ threadDelay delay
            return $ Just nextBackoff
