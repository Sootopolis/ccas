package ccas.utils

import ccas.utils.client.ChessComClient

object ApiConcurrency {

  /** Recommended cap on concurrent fibers for API-bound `foreachPar` against the given client. 2x the gate's
    * `maxPermits`: keeps the gate saturated while the next batch preps work, without queuing hundreds of fibers
    * behind it. See commit 6706d5ef.
    */
  def fiberCap(client: ChessComClient): Int = client.maxPermits * 2
}
