package dev.profunktor.redis4cats.otel4s

trait TracedModifiers[F[_], K, V] {
  type Self

  /** Modifies the current [[WrappingHelpers]]. */
  def withHelpers(f: WrappingHelpers[K, V] => WrappingHelpers[K, V]): Self

  /** Modifies the current [[CommandWrapper]]. */
  def withWrapper(f: CommandWrapper[F] => CommandWrapper[F]): Self
}
