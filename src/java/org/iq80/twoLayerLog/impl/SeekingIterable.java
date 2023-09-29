package org.iq80.twoLayerLog.impl;

import java.util.Map.Entry;

public interface SeekingIterable<K, V>
        extends Iterable<Entry<K, V>>
{
    @Override
    SeekingIterator<K, V> iterator();
}
