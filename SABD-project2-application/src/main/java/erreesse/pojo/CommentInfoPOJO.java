package erreesse.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class CommentInfoPOJO implements Serializable {

    private long approvedDate;
    private String articleID;

    private long commentID;
    private String commentType;
    private long createDate;
    private int depth;

    private boolean editorsSelection;
    private long inReplyTo;

    private long recommendations;
    private String userDisplayName;
    private long userID;



    public static CommentInfoPOJO parseFromStringLine(String line) {
        String[] tokens = line.split(",");

        CommentInfoPOJO cip = null;
        try {
            cip = new CommentInfoPOJO();
            cip.setApprovedDate(Long.parseLong(tokens[0]));
            cip.setArticleID(tokens[1]);

            cip.setCommentID(Long.parseLong(tokens[3]));
            cip.setCommentType(tokens[4]);
            cip.setCreateDate(Long.parseLong(tokens[5]));

            cip.setDepth(Integer.parseInt(tokens[6]));
            cip.setEditorsSelection(Boolean.parseBoolean(tokens[7].toLowerCase()));

            cip.setInReplyTo(Long.parseLong(tokens[8]));
            cip.setRecommendations(Long.parseLong(tokens[10]));
            cip.setUserDisplayName(tokens[12]);
            cip.setUserID(Long.parseLong(tokens[13]));

        } catch (NumberFormatException e) {

        }
        return cip;

    }
}


