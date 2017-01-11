package org.apache.flink.cep.pattern.functions;

import org.apache.flink.api.common.functions.FilterFunction;

public abstract class BiFilterFunction<T> implements FilterFunction<T> {

	protected final FilterFunction<T> left;
	protected final FilterFunction<T> right;

	public BiFilterFunction(final FilterFunction<T> left, final FilterFunction<T> right) {
		this.left = left;
		this.right = right;
	}

	public FilterFunction<T> getLeft() {
		return left;
	}

	public FilterFunction<T> getRight() {
		return right;
	}
}
