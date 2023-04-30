package name.djsweet.query.tree;

// These would all be in Kotlin, except for limitations in
// the design of Kotlin as mentioned in the per-item comments.
public final class QPTrieUtils {
    // Kotlin does not have a way to express switch fallthrough,
    // hence this being written in Java.
    //
    // The hope with this crazy switch dispatch is that modern
    // CPUs will be able to speculatively execute their way to
    // victory over multiple if statements. The switch is used
    // to effectively jump over potential out-of-bounds access
    // without otherwise incurring weird branches.
    public static int offsetForNybble(byte[] values, byte nybble) {
        switch (values.length) {
            case 16:
                if (values[15] == nybble) {
                    return 15;
                }
                // fallthrough
            case 15:
                if (values[14] == nybble) {
                    return 14;
                }
                // fallthrough
            case 14:
                if (values[13] == nybble) {
                    return 13;
                }
                // fallthrough
            case 13:
                if (values[12] == nybble) {
                    return 12;
                }
                // fallthrough
            case 12:
                if (values[11] == nybble) {
                    return 11;
                }
                // fallthrough
            case 11:
                if (values[10] == nybble) {
                    return 10;
                }
                // fallthrough
            case 10:
                if (values[9] == nybble) {
                    return 9;
                }
                // fallthrough
            case 9:
                if (values[8] == nybble) {
                    return 8;
                }
                // fallthrough
            case 8:
                if (values[7] == nybble) {
                    return 7;
                }
                // fallthrough
            case 7:
                if (values[6] == nybble) {
                    return 6;
                }
                // fallthrough
            case 6:
                if (values[5] == nybble) {
                    return 5;
                }
                // fallthrough
            case 5:
                if (values[4] == nybble) {
                    return 4;
                }
                // fallthrough
            case 4:
                if (values[3] == nybble) {
                    return 3;
                }
                // fallthrough
            case 3:
                if (values[2] == nybble) {
                    return 2;
                }
                // fallthrough
            case 2:
                if (values[1] == nybble) {
                    return 1;
                }
                // fallthrough
            case 1:
                if (values[0] == nybble) {
                    return 0;
                }
                break;
        }
        return -1;
    }
}
