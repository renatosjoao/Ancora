package org.l3s;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
 
import java.util.*;
 
/**
 * Serializable item set. Items are {@link Text} objects. Items in an item set are sorted in frequency order. Call
 * the first item in an item set the head and the remaining items the tail.
 */
public class ItemSet extends ArrayWritable implements WritableComparable<ItemSet>, Iterable<String> {
   public ItemSet() {
      super(Text.class);
      set(new ItemSet[]{});
   }
 
   public ItemSet(String... items) {
      this();
      int n = items.length;
      Text[] textItems = new Text[n];
      for (int i = 0; i < n; i++)
         textItems[i] = new Text(items[i]);
      set(textItems);
   }
 
   public ItemSet(Text... items) {
      this();
      set(items);
   }
 
   public ItemSet(ItemSet itemSet) {
      this(itemSet.items());
   }
 
   public ItemSet(Text head, ItemSet tail) {
      this();
      // Prepend the head to the items in the tail.
      String[] tailItems = tail.items();
      int n = tailItems.length;
      Text[] items = new Text[n + 1];
      items[0] = new Text(head);
      for (int i = 0; i < n; i++)
         items[i + 1] = new Text(tailItems[i]);
      set(items);
   }
 
   public String toString() {
      return StringUtils.join(items(), " ");
   }
 
   public Text[] toArray() {
      return (Text[]) super.toArray();
   }
 
   /**
    * Sort item sets lexicographically.
    *
    * @param that item set to compare to this one
    * @return -1 if this precedes, +1 if that precedes, 0 if they are equal
    */
   public int compareTo(ItemSet that) {
      return toString().compareTo(that.toString());
   }
 
   /**
    * Get the string representation of the items in this set.
    *
    * @return an array of items or an empty array if the item set is empty
    */
   public String[] items() {
      return toStrings();
   }
 
 
   public Text[] textItems() {
      return (Text[]) get();
   }
 
   @Override
   public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;
 
      ItemSet that = (ItemSet) other;
      return Arrays.equals(items(), that.items());
   }
 
   @Override
   public int hashCode() {
      return Arrays.hashCode(items());
   }
 
   /**
    * Iterate over the items in the set.
    *
    * @return iterator over the items
    */
   public Iterator<String> iterator() {
      final String[] items = items();
      return new Iterator<String>() {
         private int i = 0;
 
         public boolean hasNext() {
            return i < items.length;
         }
 
         public String next() {
            return items[i++];
         }
 
         public void remove() {
            throw new UnsupportedOperationException();
         }
      };
   }
 
   /**
    * Sort the elements of the item set in place.
    */
   public void sort() {
      Arrays.sort(get());
   }
 
   /**
    * Remove the first item from the set and return it, e.g. for {a b c} this returns "a" and changes the contents of
    * the set to {b c}. This operation is undefined for an empty item set.
    *
    * @return the first item in the set
    */
   public Text extractHead() {
      String[] items = items();
      int n = items.length;
      Text[] tail = new Text[n - 1];
      for (int i = 1; i < n; i++)
         tail[i - 1] = new Text(items[i]);
      set(tail);
      return new Text(items[0]);
   }
 
   public boolean subsumes(ItemSet that) {
      // TODO Implement more efficient search that uses the item ordering.
      Set<String> thisItems = new HashSet<String>();
      Collections.addAll(thisItems, items());
      Set<String> thatItems = new HashSet<String>();
      Collections.addAll(thatItems, that.items());
      return thisItems.containsAll(thatItems);
   }
}